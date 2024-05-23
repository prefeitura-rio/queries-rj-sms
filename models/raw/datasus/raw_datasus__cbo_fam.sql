{{
    config(
        schema="brutos_datasus",
        alias="cbo_fam",
    )
}}

with source as (
      select * from {{ source('brutos_datasus_staging', 'cbo_fam') }}
),
renamed as (
    select
        chave as id_cbo_familia,
        ds_regra as descricao,
        _data_carga,
        _data_snapshot

    from source
)
select * from renamed
