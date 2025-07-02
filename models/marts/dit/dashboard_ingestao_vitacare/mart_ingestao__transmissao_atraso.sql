{{ 
    config(
        alias='transmissao_atraso',
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
    where est.prontuario_versao = 'vitacare'
      and est.prontuario_episodio_tem_dado = 'sim'
      and id_cnes is not null

    union all

    select 'nao-informado', 'nao-se-aplica', 'CNES não informado'
  ),

  -- -----------------------------
  -- Transmissões
  -- -----------------------------
  transmissoes_individuais_paciente as (
    select
      source_id,
      coalesce(nullif(json_extract_scalar(trans.data,'$.cnes'), ''), 'nao-informado') as id_cnes,
      safe_cast(
        TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date
      ) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) as dia_ingestao,

      TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as momento_ocorrencia,
      datalake_loaded_at as momento_ingestao,
    from {{ source('brutos_prontuario_vitacare_staging', 'paciente_continuo') }} trans
    where DATE(datalake_loaded_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  ),
  transmissoes_individuais_atendimento as (
    select
      source_id,
      coalesce(nullif(json_extract_scalar(trans.data,'$.unidade_cnes'), ''), 'nao-informado') as id_cnes,
      safe_cast(
        TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date
      ) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) as dia_ingestao,

      TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as momento_ocorrencia,
      datalake_loaded_at as momento_ingestao,
    from {{ source('brutos_prontuario_vitacare_staging', 'atendimento_continuo') }} trans
    where DATE(datalake_loaded_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  ),

  -- -----------------------------
  -- Junção
  -- -----------------------------
  transmissoes_individuais as (
    select
      *,
      -- DATE_DIFF(dia_ingestao, dia_ocorrencia, DAY) as atraso_transmissao
      TIMESTAMP_DIFF(momento_ingestao, momento_ocorrencia, MINUTE) as atraso_transmissao
    from transmissoes_individuais_paciente

    union all

    select *,
      -- DATE_DIFF(dia_ingestao, dia_ocorrencia, DAY) as atraso_transmissao
      TIMESTAMP_DIFF(momento_ingestao, momento_ocorrencia, DAY) as atraso_transmissao
    from transmissoes_individuais_atendimento
  ),

  transmissoes_agrupadas_mes as (
    select
      id_cnes,
      round(avg(atraso_transmissao), 0) as media_atraso,
      max(atraso_transmissao) as max_atraso
    from transmissoes_individuais
    -- Não precisamos filtrar por data aqui porque já estamos pegando
    -- só dados do último mês
    group by 1
  ),
  transmissoes_agrupadas_semana as (
    select
      id_cnes,
      round(avg(atraso_transmissao), 0) as media_atraso,
      max(atraso_transmissao) as max_atraso
    from transmissoes_individuais
    where dia_ingestao >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    group by 1
  ),
  transmissoes_agrupadas_dia as (
    select
      id_cnes,
      round(avg(atraso_transmissao), 0) as media_atraso,
      max(atraso_transmissao) as max_atraso
    from transmissoes_individuais
    where dia_ingestao >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    group by 1
  )

select
  unidades.area_programatica,
  unidades.id_cnes,
  unidades.nome_fantasia,
  transmissoes_agrupadas_dia.media_atraso as media_atraso_dia,
  transmissoes_agrupadas_semana.media_atraso as media_atraso_semana,
  transmissoes_agrupadas_mes.media_atraso as media_atraso_mes
from unidades
  left join transmissoes_agrupadas_mes using(id_cnes)
  left join transmissoes_agrupadas_semana using(id_cnes)
  left join transmissoes_agrupadas_dia using(id_cnes)
order by media_atraso_dia desc
