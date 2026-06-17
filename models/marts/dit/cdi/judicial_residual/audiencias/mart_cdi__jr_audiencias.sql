{{ 
  config(
    schema = "projeto_cdi",
    alias  = "jr_audiencias",
    materialized = "table",
    meta={"owner": "karen"}
) }}

with base as (

  select
    processo_rio,

    coalesce(
      initcap(trim(regexp_replace(orgao_para_subsidiar, r'\s+', ' '))),
      'Não informado'
    ) as orgao_para_subsidiar,

    entrada_gat3 as data_ref,
    retorno as data_retorno,
    vencimento as dt_venc,

    situacao,

    case
      when situacao is null then 'Não informado'
      else initcap(lower(situacao))
    end as situacao_exibicao

  from {{ ref('int_cdi__judicial_residual') }}

  where solicitacao = 'audiência'
    and entrada_gat3 is not null

),

classificada as (

  select
    *,

    case
      when situacao like 'RESOLVIDO%' then 'Resolvido'
      when situacao like 'PENDENTE%' then 'Pendente'
      when situacao is null then 'Não informado'
      else 'Não informado'
    end as status_audiencia

  from base

),

calc as (

  select
    *,

    date_diff(dt_venc, current_date(), day) as dias_para_vencer,

    case
      when dt_venc is null then null
      when date_diff(dt_venc, current_date(), day) < 0
        then concat('Vencida há ', abs(date_diff(dt_venc, current_date(), day)), ' dias')
      when date_diff(dt_venc, current_date(), day) = 0
        then 'Vence hoje'
      else concat('Em ', date_diff(dt_venc, current_date(), day), ' dias')
    end as prazo_legivel,

    format_date('%Y-%m', date_trunc(data_ref, month)) as ano_mes

  from classificada

),

resumo_cards as (

  select
    coalesce(safe_divide(countif(status_audiencia = 'Resolvido'), count(*)), 0) as pct_resolvidas,
    coalesce(safe_divide(countif(status_audiencia = 'Pendente'), count(*)), 0) as pct_pendentes,
    coalesce(cast(avg(date_diff(data_retorno, data_ref, day)) as int64), 0) as tempo_medio_dias

  from calc

)

select
  c.processo_rio,
  c.orgao_para_subsidiar,
  c.data_ref,
  c.dt_venc,
  c.status_audiencia,
  c.situacao_exibicao as situacao,
  c.dias_para_vencer,
  c.prazo_legivel,
  c.ano_mes,

  r.pct_resolvidas,
  r.pct_pendentes,
  r.tempo_medio_dias

from calc c

left join resumo_cards r
  on true

order by c.data_ref