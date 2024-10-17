{{
    config(
        alias="uso_sistema",
        schema="app_historico_clinico",
        materialized="table",
    )
}}

with
  historico as (
    select 
        user_id,
        method,
        path,
        timestamp,
        status_code
    from {{ ref('raw_monitoramento__userhistory') }}
    where path like '%header%'
    order by timestamp asc
  ),
  dados as (
    select 
        id,
        name as usuario_nome,
        cpf as usuario_cpf,
        is_2fa_activated as ativou_2fa,
        data_source_id as usuario_cnes,
        created_at,
        updated_at,
    from {{ ref('raw_monitoramento__userinfo') }}
    where cpf is not null
  ),
  historico_agrupado as (
    select 
        user_id,
        array_agg(
            struct(
                replace(path, '/frontend/patient/header/', '') as cpf_alvo,
                timestamp as momento_consulta
            )
        ) as consultas
    from historico
    group by user_id
  ),
  consolidado as (
    select 
        dados.* except(id),
        historico_agrupado.consultas
    from dados
        left join historico_agrupado on dados.id = historico_agrupado.user_id
    
  )
select *
from consolidado
order by usuario_nome asc