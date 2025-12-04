-- noqa: disable=LT08
{{
  config(
    enabled=true,
    schema="brutos_centralderegulacao_mysql",
    alias="cirgurias_eletivas_sisare",
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['paciente_cns', 'id_procedimento', 'data_entrada'],
    partition_by={
      "field": "data_entrada",
      "data_type": "date",
      "granularity": "month",
    },
cluster_by = ['procedimento', 'id_cnes_unidade_origem', 'unidade_origem_ap'],
on_schema_change = 'sync_all_columns'
)
}}

with

    {% if is_incremental() %}
    max_data_extracao as (
        select max(data_extracao) from {{ this }}
    ),
    {% endif %}

    source as (
        select
            safe_cast(cns_pac as int64) as paciente_cns,

            upper(trim(no_procedimento)) as procedimento,
            safe_cast(id_proced as int64) as id_procedimento,
            safe_cast(cod_proced as int64) as cod_procedimento,

            desc_status as status,
            safe_cast(id_status as int64) as id_status,

            safe_cast(split(hosp_origem, " - ") [offset(2)] as in64) as unidade_origem_ap,
            upper(trim(split(hosp_origem, " - ") [offset(1)])) as unidade_origem,
            safe_cast(split(hosp_origem, " - ") [offset(0)] as int64) as id_cnes_unidade_origem,

            safe_cast(data_entrada as date) as data_entrada,
            safe_cast(ultima_movimentacao as timestamp) as data_atualizacao_registro,
            safe_cast(data_particao date) as data_extracao

        from {{ source("brutos_centralderegulacao_mysql_staging", "vw_eletivas_sisare") }}

            {% if is_incremental() %}
        where 1 = 1
            and safe_cast(data_particao as date) >= (select max_data_extracao from max_data_extracao)
            {% endif %}
    )

select
    *,
    row_number() over (
        partition by paciente_cns, id_procedimento, data_entrada
        order by data_atualizacao_registro desc
    ) as rn
from source
    qualify rn = 1
