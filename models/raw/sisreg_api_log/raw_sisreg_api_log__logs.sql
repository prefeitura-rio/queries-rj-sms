{{
  config(
    enabled = true,
    materialized = 'incremental',
    schema = "brutos_sisreg_api_log",
    alias = "logs",
    partition_by = {
      "field": "data_inicial",
      "data_type": "date",
      "granularity": "month"
    },
    incremental_strategy = 'insert_overwrite',
    partitions = ['data_inicial'],
    cluster_by = ['tabela','run_id'],
    on_schema_change = 'sync_all_columns'
  )
}}

with
extracoes as (
  select
      run_id,
      datetime(as_of) as run_inicio,
      datetime(validation_date) as run_fim,
      environment as run_ambiente,
      bq_table as tabela,
      bq_dataset,
      data_inicial,
      data_final,
      cast(completed as bool) as completed
  from {{ source("brutos_sisreg_api_log_staging", "marcacoes") }}
  {% if is_incremental() %}
    where
      date(data_particao) >= date_sub(current_date(), interval 1 month)
      and run_id not in (select run_id from {{this}})
  {% endif %}

  union all

  select
      run_id,
      datetime(as_of) as run_inicio,
      datetime(validation_date) as run_fim,
      environment as run_ambiente,
      bq_table as tabela,
      bq_dataset,
      data_inicial,
      data_final,
      cast(completed as bool) as completed
  from {{ source("brutos_sisreg_api_log_staging", "solicitacoes") }}
  {% if is_incremental() %}
  where 
      date(data_particao) >= date_sub(current_date(), interval 1 month)
      and run_id not in (select run_id from {{this}})
  {% endif %}
),

n_rows as (
  select
      "solicitacoes" as tabela,
      s.run_id,
      count(*) as nrows
  from {{ source('brutos_sisreg_api_staging', 'solicitacoes') }} s
  {% if is_incremental() %}
  where 
      date(data_particao) >= date_sub(current_date(), interval 1 month)
      and run_id in (select run_id from extracoes)
  {% endif %}
  group by 1,2

  union all

  select
      "marcacoes" as tabela,
      m.run_id,
      count(*) as nrows
  from {{ source('brutos_sisreg_api_staging', 'marcacoes') }} m
  {% if is_incremental() %}
  where 
      date(data_particao) >= date_sub(current_date(), interval 1 month)
      and run_id in (select run_id from extracoes)
  {% endif %}
  group by 1,2
),

consolidado as (
    select
        e.*,
        n.nrows
    from extracoes e
    left join n_rows n
    using (tabela, run_id)
)

select *
from consolidado
