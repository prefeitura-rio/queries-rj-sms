{{ config(
  schema = "projeto_cdi",
  alias  = "jr_desempenho_proximas_demandas",
  materialized = "table",
  meta={"owner": "karen"}
) }}

with base as (

  select
    id,
    processo_rio,

    coalesce(
      initcap(orgao_para_subsidiar),
      'Não informado'
    ) as orgao_para_subsidiar,

    entrada_gat3 as data_entrada,
    retorno as data_retorno,
    prazo_dias,

    date_add(entrada_gat3, interval prazo_dias day) as data_vencimento,

    initcap(lower(situacao)) as situacao

  from {{ ref('int_cdi__judicial_residual') }}

  where entrada_gat3 is not null
    and situacao like 'PENDENTE%'

),

calc as (

  select
    *,

    date_diff(data_vencimento, current_date(), day) as dias_para_vencer

  from base

),

classificada as (

  select
    *,

    case
      when data_vencimento is null then 'Sem prazo definido'
      when dias_para_vencer < 0
        then concat('Vencida há ', abs(dias_para_vencer), ' dias')
      when dias_para_vencer = 0
        then 'Vence hoje'
      else concat('Em ', dias_para_vencer, ' dias')
    end as prazo_legivel,

    case
      when data_vencimento is null then 'Sem prazo definido'
      when dias_para_vencer < 0 then 'Vencida'
      when dias_para_vencer between 0 and 7 then 'A vencer (≤7 dias)'
      when dias_para_vencer between 8 and 15 then 'A vencer (8–15 dias)'
      else 'Dentro do Prazo (>15 dias)'
    end as status_vencimento

  from calc

),

dedup as (

  select * except (rn)
  from (
    select
      c.*,
      row_number() over (
        partition by id
        order by
          case when data_vencimento is null then 1 else 0 end,
          data_vencimento asc,
          data_entrada desc
      ) as rn
    from classificada c
  )
  where rn = 1

)

select
  coalesce(processo_rio, 'Não informado') as processo_rio,
  orgao_para_subsidiar,
  data_entrada,
  data_vencimento,
  situacao,
  dias_para_vencer,
  prazo_legivel,
  status_vencimento

from dedup

order by data_vencimento asc