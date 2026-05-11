{{
    config(
        alias = "arcgis__layer_versions",
        materialized = "table",
        schema = "brutos_arcgis",
        tags = ["subpav", "arcgis", "onde_ser_atendido","osa"]
    )
}}

with source as (
    select *
    from {{ source("arcgis_staging", "arcgis_layer_versions") }}
),

renamed as (
    select
        cast(versao_id as string) as versao_id,
        cast(endpoint_url as string) as endpoint_url,
        safe_cast(layer_id as int64) as layer_id,
        cast(layer_name as string) as layer_name,
        cast(layer_hash as string) as layer_hash,

        safe_cast(features_count as int64) as features_count,
        safe_cast(min_objectid as int64) as min_objectid,
        safe_cast(max_objectid as int64) as max_objectid,

        cast(arcgis_geometry_type as string) as arcgis_geometry_type,
        safe_cast(arcgis_spatial_reference_wkid as int64) as arcgis_spatial_reference_wkid,
        safe_cast(arcgis_last_edit_date as int64) as arcgis_last_edit_date,

        safe_cast(data_extracao as timestamp) as data_extracao,
        cast(status as string) as status,
        cast(observacao as string) as observacao,
        safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at,

        cast(dataset_id as string) as dataset_id,
        cast(table_id as string) as table_id
    from source
)

select *
from renamed
