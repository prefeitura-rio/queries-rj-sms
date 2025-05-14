{{
    config(
        schema="brutos_geo_pgeo3",
        alias="estabelecimentos_coordenadas",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    source as (
        select
            lpad(id_cnes, 7, '0') as id_cnes,
            lpad(endereco_cep, 8, '0') as endereco_cep,

            endereco_bairro,
            endereco_logradouro,
            endereco_numero,

            safe_cast(latitude_cep as float64) as latitude_cep,
            safe_cast(longitude_cep as float64) as longitude_cep,
            safe_cast(latitude_addr as float64) as latitude_addr,
            safe_cast(longitude_addr as float64) as longitude_addr,
            safe_cast(latitude_api as float64) as latitude_api,
            safe_cast(longitude_api as float64) as longitude_api,

            safe_cast(data_extracao as datetime) as data_extracao,
            safe_cast(ano_particao as int64) as ano_particao,
            safe_cast(mes_particao as int64) as mes_particao,
            safe_cast(data_particao as date) as data_particao

        from {{ source("brutos_geo_pgeo3_staging", "estabelecimentos_coordenadas") }}

        qualify row_number() over (
            partition by id_cnes
            order by data_particao desc
        ) = 1

    )

select *
from source