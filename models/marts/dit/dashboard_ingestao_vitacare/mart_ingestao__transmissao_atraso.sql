{{ 
    config(
        alias='transmissao_atraso',
        materialized='table',
    ) 
}}

WITH

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
    SELECT 
      source_id,
      COALESCE(nullif(json_extract_scalar(trans.data,'$.cnes'), ''), 'nao-informado') AS id_cnes,
      safe_cast(
        TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date
      ) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) AS dia_ingestao,
    FROM {{ source('brutos_prontuario_vitacare_staging', 'paciente_continuo') }} trans
    WHERE DATE(datalake_loaded_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  ),
  transmissoes_individuais_atendimento as (
    SELECT 
      source_id,
      COALESCE(nullif(json_extract_scalar(trans.data,'$.unidade_cnes'), ''), 'nao-informado') AS id_cnes,
      safe_cast(
        TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date
      ) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) AS dia_ingestao,
    FROM {{ source('brutos_prontuario_vitacare_staging', 'atendimento_continuo') }} trans
    WHERE DATE(datalake_loaded_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  ),

  -- -----------------------------
  -- Junção
  -- -----------------------------
  transmissoes_individuais as (
    select *
    from transmissoes_individuais_paciente
    union all
    select *
    from transmissoes_individuais_atendimento
  ),

  transmissoes_agrupadas_mes as (
    select id_cnes, round(avg(atraso_transmissao),2) as media_atraso, max(atraso_transmissao) as max_atraso
    from transmissoes_individuais
    group by 1
  ),

  transmissoes_agrupadas_semana as (
    select id_cnes, round(avg(atraso_transmissao),2) as media_atraso, max(atraso_transmissao) as max_atraso
    from transmissoes_individuais
    WHERE DATE(momento_ingestao) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    group by 1
  ),

  transmissoes_agrupadas_dia as (
    select id_cnes, round(avg(atraso_transmissao),2) as media_atraso, max(atraso_transmissao) as max_atraso
    from transmissoes_individuais
    WHERE DATE(momento_ingestao) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    group by 1
  )
  select 
    unidades.area_programatica,
    transmissoes_agrupadas_mes.id_cnes,
    unidades.nome_fantasia,
    transmissoes_agrupadas_dia.media_atraso as media_atraso_dia,
    transmissoes_agrupadas_semana.media_atraso as media_atraso_semana,
    transmissoes_agrupadas_mes.media_atraso as media_atraso_mes
  from unidades
    left join transmissoes_agrupadas_mes using(id_cnes)
    left join transmissoes_agrupadas_semana using(id_cnes)
    left join transmissoes_agrupadas_dia using(id_cnes)
