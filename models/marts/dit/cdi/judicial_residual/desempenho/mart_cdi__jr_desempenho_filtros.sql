{{ config(
  schema = "projeto_cdi",
  alias  = "jr_desempenho_filtros",
  materialized = "view",
  meta={"owner": "karen"}
) }}

select distinct
  coalesce(orgao, 'Não informado') as orgao,

  coalesce(initcap(solicitacao), 'Não informado') as tipo_solicitacao,

  case
    when regexp_contains(area, r'^\d+(\.\d+)?$')
      then regexp_replace(area, r'\.', '')
    else 'Não informado'
  end as codigo_ap,

  coalesce(area, 'Não informado') as area,

  coalesce(situacao, 'Não informado') as situacao,

  date_trunc(entrada_gat3, month) as ano_mes_dt

from {{ ref('int_cdi__judicial_residual') }}

where entrada_gat3 is not null