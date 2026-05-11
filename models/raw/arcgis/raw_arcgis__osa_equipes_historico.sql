{{
    config(
        alias = "arcgis__osa_equipes_historico",
        materialized = "table",
        schema = "brutos_arcgis",
        tags = ["subpav", "arcgis", "onde_ser_atendido","osa"]
    )
}}

with source as (
    select *
    from {{ source("arcgis_staging", "osa__equipes_historico") }}
),

renamed as (
    select
        cast(versao_id as string) as versao_id,

        -- Data de referência da extração; não identifica uma versão única.
        safe_cast(data_versao as date) as data_versao,

        -- Timestamp usado para ordenar múltiplas versões no mesmo dia.
        safe_cast(data_extracao as timestamp) as data_extracao,

        cast(endpoint_url as string) as endpoint_url,
        safe_cast(layer_id as int64) as layer_id,
        cast(layer_name as string) as layer_name,

        safe_cast(objectid as int64) as objectid,
        cast(global_id as string) as global_id,

        safe_cast(cap as int64) as cap,
        safe_cast(ap as int64) as ap,

        lpad(regexp_replace(cast(cnes as string), r'\D', ''), 7, '0') as cnes,
        safe_cast(cod_equipe as int64) as cod_equipe,
        regexp_replace(cast(cod_ine as string), r'\D', '') as cod_ine,
        safe_cast(cod_area as int64) as cod_area,

        cast(nome_area as string) as nome_area,
        cast(nome_fantasia as string) as nome_fantasia,
        cast(tipo_unidade_aps as string) as tipo_unidade_aps,
        cast(bairro as string) as bairro,
        cast(logradouro as string) as logradouro,
        cast(numero as string) as numero,
        cast(complemento as string) as complemento,
        lpad(regexp_replace(cast(cod_cep as string), r'\D', ''), 8, '0') as cod_cep,
        cast(telefone as string) as telefone,
        cast(email as string) as email,

        date(timestamp_millis(safe_cast(safe_cast(dt_ativa as float64) as int64))) as dt_ativa,
        safe_cast(tipo_eqp as int64) as tipo_eqp,

        cast(geometry_geojson as string) as geometry_geojson,
        cast(feature_json as string) as feature_json,
        cast(feature_hash as string) as feature_hash,
        cast(layer_hash as string) as layer_hash,
        safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
    from source
),

deduplicado as (
    select *
    from renamed
    qualify row_number() over (
        partition by versao_id, objectid, feature_hash
        order by datalake_loaded_at desc
    ) = 1
)

select *
from deduplicado
