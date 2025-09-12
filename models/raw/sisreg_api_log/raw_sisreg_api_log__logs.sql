{{
  config(
    enabled = true,
    materialized = 'incremental',
    schema = "brutos_sisreg_api_log",
    alias = "logs",
    partition_by = {
      "field": "data_particao",
      "data_type": "date",
      "granularity": "month"
    },
    incremental_strategy = 'insert_overwrite',
    partitions = ['data_particao'],
    cluster_by = ['bq_table','run_id'],
    on_schema_change = 'sync_all_columns'
  )
}}

{%- set months_lookback = var('months_lookback', 1) -%}
{%- set min_build_date = "DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL " ~ months_lookback ~ " MONTH), MONTH)" -%}

with
extracoes as (
  select
      run_id,
      datetime(as_of) as run_inicio,
      datetime(validation_date) as run_fim,
      environment as run_ambiente,
      bq_table,
      bq_dataset,
      data_inicial,
      data_final,
      completed,
      ano_particao,
      mes_particao,
      safe.parse_date('%Y-%m-%d', data_particao) as data_particao
  from {{ source("brutos_sisreg_api_log_staging", "marcacoes") }}
  where 1=1
    and safe.parse_date('%Y-%m-%d', data_particao) is not null
    {% if is_incremental() %}
      and safe.parse_date('%Y-%m-%d', data_particao) >= {{ min_build_date }}
    {% endif %}

  union all

  select
      run_id,
      datetime(as_of) as run_inicio,
      datetime(validation_date) as run_fim,
      environment as run_ambiente,
      bq_table,
      bq_dataset,
      data_inicial,
      data_final,
      completed,
      ano_particao,
      mes_particao,
      safe.parse_date('%Y-%m-%d', data_particao) as data_particao
  from {{ source("brutos_sisreg_api_log_staging", "solicitacoes") }}
  where 1=1
    and safe.parse_date('%Y-%m-%d', data_particao) is not null
    {% if is_incremental() %}
      and safe.parse_date('%Y-%m-%d', data_particao) >= {{ min_build_date }}
    {% endif %}
),

n_rows as (
  select
      "solicitacoes" as tabela,
      safe.parse_date('%Y-%m-%d', s.data_particao) as data_particao,
      s.run_id,
      count(*) as nrows
  from {{ source('brutos_sisreg_api_staging', 'solicitacoes') }} s
  {% if is_incremental() %}
    where safe.parse_date('%Y-%m-%d', s.data_particao) >= {{ min_build_date }}
  {% endif %}
  group by 1,2,3

  union all

  select
      "marcacoes" as tabela,
      safe.parse_date('%Y-%m-%d', m.data_particao) as data_particao,
      m.run_id,
      count(*) as nrows
  from {{ source('brutos_sisreg_api_staging', 'marcacoes') }} m
  {% if is_incremental() %}
    where safe.parse_date('%Y-%m-%d', m.data_particao) >= {{ min_build_date }}
  {% endif %}
  group by 1,2,3
),

consolidado as (
    select
        e.*,
        n.nrows
    from extracoes e
    left join n_rows n
    using (data_particao, run_id)
)

select *
from consolidado
{% if is_incremental() %}
  where data_particao >= {{ min_build_date }}
{% endif %}
