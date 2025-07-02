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
    from {{ ref('dim_estabelecimento') }} est
    where est.prontuario_versao = 'vitacare' and est.prontuario_episodio_tem_dado = 'sim'
    union all
    select 'nao-informado', 'nao-se-aplica', 'CNES não informado'
  ),

  -- -----------------------------
  -- Transmissões
  -- -----------------------------
  transmissoes_individuais_paciente as (
    select
      source_id,
      coalesce(nullif(json_extract_scalar(trans.data,'$.cnes'), ''), 'nao-informado') AS id_cnes,
      safe_cast(
        TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date
      ) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) AS dia_ingestao,
    from {{ source('brutos_prontuario_vitacare_staging', 'paciente_continuo') }} trans
    where DATE(datalake_loaded_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  ),
  transmissoes_individuais_atendimento as (
    select
      source_id,
      coalesce(nullif(json_extract_scalar(trans.data,'$.unidade_cnes'), ''), 'nao-informado') AS id_cnes,
      safe_cast(
        TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date
      ) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) AS dia_ingestao,
    from {{ source('brutos_prontuario_vitacare_staging', 'atendimento_continuo') }} trans
    where DATE(datalake_loaded_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
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
    where DATE(dia_ingestao) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    group by 1, 2
  ),
  transmissoes_agrupadas_dia as (
    select id_cnes, tipo_registro, sum(quantidade) as quantidade
    from transmissoes_agrupadas
    where DATE(dia_ingestao) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
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

      coalesce(dia.quantidade,0) as pacientes_ultimo_dia,
      struct(
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

      coalesce(dia.quantidade,0) as atendimentos_ultimo_dia,
      struct(
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
      analise_atendimento.atendimentos_ultimo_dia,
      analise_atendimento.media_atendimentos
    from analise_paciente
      inner join analise_atendimento using(area_programatica, id_cnes, nome_fantasia)
    order by id_cnes asc
  )

select *
from final
order by area_programatica asc, id_cnes asc
