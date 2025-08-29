{{ 
    config(
        alias='transmissao_atividade',
        materialized='table',
    ) 
}}


with

  -- -----------------------------
  -- Dados de Unidades
  -- -----------------------------
  unidades as (
    select
      id_cnes,
      area_programatica,
      nome_fantasia
    from {{ ref('dim_estabelecimento') }}
    where prontuario_versao = 'vitacare'
      and prontuario_episodio_tem_dado = 'sim'
      and id_cnes is not null

    union all

    select 'nao-informado', 'nao-se-aplica', 'CNES não informado'
  ),

  -- -----------------------------
  -- Transmissões
  -- -----------------------------
  initial_transmissoes_individuais_paciente as (
    select
      source_id,
      coalesce(nullif(json_extract_scalar(trans.data,'$.cnes'), ''), 'nao-informado') as id_cnes,
      safe_cast(
        TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date
      ) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) as dia_ingestao,
    from {{ source('brutos_prontuario_vitacare_api_staging', 'paciente_continuo') }} trans
    where DATE(datalake_loaded_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  ),
  transmissoes_individuais_paciente as (
    select *
    from initial_transmissoes_individuais_paciente
    -- Pega somente ocorrências dos último mês
    where current_datetime("America/Sao_Paulo") >= dia_ocorrencia
      and dia_ocorrencia >=
        -- [Ref] https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
        -- Transforma dia em 01 (i.e. conta a partir do dia 1º de 30 dias atrás)
        date_trunc(
          -- Subtrai 1 mês do dia em que o código está sendo executado
          date(
            datetime_sub(
              current_datetime("America/Sao_Paulo"),
              interval 30 day
            )
          ),
          month
        )
  ),

  initial_transmissoes_individuais_atendimento as (
    select
      source_id,
      coalesce(nullif(json_extract_scalar(trans.data,'$.unidade_cnes'), ''), 'nao-informado') as id_cnes,
      safe_cast(
        TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date
      ) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) as dia_ingestao,
    from {{ source('brutos_prontuario_vitacare_api_staging', 'atendimento_continuo') }} trans
    where DATE(datalake_loaded_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  ),
  transmissoes_individuais_atendimento as (
    select *
    from initial_transmissoes_individuais_atendimento
    -- Pega somente ocorrências dos último mês
    where current_datetime("America/Sao_Paulo") >= dia_ocorrencia
      and dia_ocorrencia >= date_trunc(
        date(
          datetime_sub(current_datetime("America/Sao_Paulo"), interval 30 day)
        ),
        month
      )
  ),

  -- -----------------------------
  -- Junção
  -- -----------------------------
  transmissoes_individuais as (
    select *, 'paciente' as tipo_registro
    from transmissoes_individuais_paciente

    union all

    select *, 'atendimento' as tipo_registro
    from transmissoes_individuais_atendimento
  ),


  -- -----------------------------
  -- Agrupamentos
  -- -----------------------------
  transmissoes_agrupadas as (
    select id_cnes, dia_ingestao, tipo_registro, count(*) as quantidade
    from transmissoes_individuais
    group by 1, 2, 3
  ),
  transmissoes_agrupadas_mes as (
    select id_cnes, tipo_registro, round(avg(quantidade),2) as quantidade
    from transmissoes_agrupadas
    -- Não precisamos filtrar por data aqui porque só estamos pegando dados
    -- do último mês já desde o começo
    group by 1, 2
  ),
  transmissoes_agrupadas_semana as (
    select id_cnes, tipo_registro, round(avg(quantidade),2) as quantidade
    from transmissoes_agrupadas
    where dia_ingestao >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    group by 1, 2
  ),
  transmissoes_agrupadas_dia as (
    select id_cnes, tipo_registro, round(avg(quantidade),2) as quantidade
    from transmissoes_agrupadas
    -- Aqui é complicado, porque eu (oi, avellar aqui) não sei quando esse código
    -- vai ser executado. Se queremos contabilizar 1 dia, queremos um dia completo,
    -- então o seguro seria pegar o dia de ontem. Mas isso pode ser executado em
    -- uma segunda-feira às 18h (e aí 'ontem' seria domingo e os dados valiosos são
    -- da própria segunda) ou num dia qualquer às 01h (e aí não faz sentido só
    -- pegar dados do próprio dia).
    -- Achei que a melhor opção, então, era calcular média entre dados *a partir de*
    -- ontem. A quantidade é, então, média de até 2 dias.
    where dia_ingestao >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    group by 1, 2
  ),


  -- -----------------------------
  -- Analise p/ Registros de Pacientes
  -- -----------------------------
  analise_paciente as (
    select
      unidades.area_programatica,
      unidades.id_cnes,
      unidades.nome_fantasia,

      struct(
        coalesce(dia.quantidade,0) as ultimos_2_dias,
        coalesce(semana.quantidade,0) as ultima_semana,
        coalesce(mes.quantidade,0) as ultimo_mes
      ) as media_pacientes
    from unidades
      left join transmissoes_agrupadas_mes as mes using(id_cnes)
      left join transmissoes_agrupadas_semana as semana
        on semana.id_cnes = mes.id_cnes
        and semana.tipo_registro = mes.tipo_registro
      left join transmissoes_agrupadas_dia as dia
        on semana.id_cnes = dia.id_cnes
        and semana.tipo_registro = dia.tipo_registro
    where semana.tipo_registro = 'paciente'
    order by id_cnes asc
  ),
  analise_atendimento as (
    select
      unidades.area_programatica,
      unidades.id_cnes,
      unidades.nome_fantasia,

      struct(
        coalesce(dia.quantidade,0) as ultimos_2_dias,
        coalesce(semana.quantidade,0) as ultima_semana,
        coalesce(mes.quantidade,0) as ultimo_mes
      ) as media_atendimentos
    from unidades
      left join transmissoes_agrupadas_mes as mes using(id_cnes)
      left join transmissoes_agrupadas_semana as semana
        on semana.id_cnes = mes.id_cnes
        and semana.tipo_registro = mes.tipo_registro
      left join transmissoes_agrupadas_dia as dia
        on semana.id_cnes = dia.id_cnes
        and semana.tipo_registro = dia.tipo_registro
    where semana.tipo_registro = 'atendimento'
    order by id_cnes asc
  ),

  -- -----------------------------
  -- Junção Horizontal de Analises
  -- -----------------------------
  final as (
    select
      analise_paciente.*,
      analise_atendimento.media_atendimentos
    from analise_paciente
      inner join analise_atendimento using(area_programatica, id_cnes, nome_fantasia)
    order by id_cnes asc
  )

select *
from final
order by area_programatica asc, id_cnes asc
