{{
    config(
        schema="brutos_datasus",
        alias="cbo",
    )
}}


with source as (
      select * from {{ source('brutos_datasus_staging', 'cbo') }}
),
renamed as (
    select
        cbo as id_cbo,
        ds_cbo as descricao,
        _data_carga,
        _data_snapshot,

    from source
)
select * from renamed
