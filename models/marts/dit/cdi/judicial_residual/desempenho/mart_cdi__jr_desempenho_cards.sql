{{ 
  config(
    schema = "projeto_cdi",
    alias  = "jr_desempenho_cards",
    materialized = "table",
    meta={"owner": "karen"}
) }}

with base as (

  select
    id,
    orgao,

    coalesce(area, 'Não informado') as area,

    case
      when regexp_contains(area, r'^\d+(\.\d+)?$')
        then regexp_replace(area, r'\.', '')
      else 'Não informado'
    end as codigo_ap,

    situacao,

    entrada_gat3 as data_entrada,
    date_trunc(entrada_gat3, month) as ano_mes_dt,
    retorno as data_retorno,
    prazo_dias,

    date_add(entrada_gat3, interval prazo_dias day) as data_vencimento,

    solicitacao

  from {{ ref('int_cdi__judicial_residual') }}

  where entrada_gat3 is not null

),

base_tipificada as (

  select
    *,

    case
      when solicitacao is null then 'Não informado'
      when regexp_contains(solicitacao, r',') then 'Múltiplas'
      else initcap(solicitacao)
    end as tipo_solicitacao

  from base

),

calc as (

  select
    *,

    case
      when data_retorno is null or data_entrada is null then null
      when data_retorno < data_entrada then null
      else date_diff(data_retorno, data_entrada, day)
    end as dias_atendimento,

    case
      when data_retorno is null then 'Pendente'
      when data_vencimento is null then 'Sem prazo'
      when data_retorno <= data_vencimento then 'Dentro do Prazo'
      else 'Fora do Prazo'
    end as status_prazo

  from base_tipificada

)

select
  id,
  data_entrada,
  data_retorno,
  data_vencimento,
  prazo_dias,
  ano_mes_dt,
  orgao,
  tipo_solicitacao,
  codigo_ap,
  area,
  situacao,
  status_prazo,
  dias_atendimento

from calc