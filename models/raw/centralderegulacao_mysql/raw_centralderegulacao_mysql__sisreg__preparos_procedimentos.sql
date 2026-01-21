-- noqa: disable=LT08
{{
  config(
    enabled=true,
    schema="brutos_centralderegulacao_mysql",
    alias="sisreg_preparos_procedimentos",
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id_cnes', 'id_procedimento_sisreg'],
    partition_by={
      "field": "data_extracao",
      "data_type": "date",
      "granularity": "month",
    },
cluster_by = ['id_cnes', 'id_procedimento_sisreg'],
on_schema_change = 'sync_all_columns'
)
}}

with
    source as (
        select
            lpad(cnesunidade, 7, "0") as id_cnes,
            lpad(codigoprocedimentointerno, 7, "0") as id_procedimento_sisreg,
            {{ process_null("preparoprocedimento") }} as procedimento_preparo,
            {{ process_null("preparoprocedimentotagshtml") }} as procedimento_preparo_html,
            
            safe_cast(data_particao as date) as data_extracao

        from
            {{ source('brutos_centralderegulacao_mysql_staging', 'tb_preparos_finais') }}

        
        {% if is_incremental() %}
        where 1 = 1
            and safe_cast(data_particao as date) >= (
                select max(data_extracao) from {{ this }}
            )
        {% endif %}
    )

select
    *,
    row_number() over (
        partition by id_cnes, id_procedimento_sisreg
        order by data_extracao desc
    ) as rn
from source
    qualify rn = 1
