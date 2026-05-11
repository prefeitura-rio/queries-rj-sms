{{
    config(
        alias = "arcgis__osa_equipes_territorio",
        materialized = "table",
        tags = ["subpav", "arcgis", "onde_ser_atendido", "geo"]
    )
}}

with versao_atual as (
    select
        versao_id,
        layer_hash,
        data_extracao
    from {{ ref("raw_arcgis__layer_versions") }}
    where status = 'loaded'
        and table_id = 'osa__equipes_historico'

    -- A versão vigente é definida pelo timestamp de extração mais recente,
    -- não por data_versao, pois podem existir múltiplos snapshots no mesmo dia.
    qualify row_number() over (
        order by data_extracao desc
    ) = 1
),

equipes as (
    select
        historico.*
    from {{ ref("raw_arcgis__osa_equipes_historico") }} as historico
    inner join versao_atual
        on historico.versao_id = versao_atual.versao_id
        and historico.layer_hash = versao_atual.layer_hash
),

geometria as (
    select
        *,

        safe.st_geogfromgeojson(geometry_geojson) is null
            and safe.st_geogfromgeojson(
                geometry_geojson,
                make_valid => true
            ) is not null as geometry_corrigida,

        safe.st_geogfromgeojson(
            geometry_geojson,
            make_valid => true
        ) as geometry

    from equipes
    where geometry_geojson is not null
        and cod_ine is not null
        and trim(cod_ine) != ''
),
tratado as (
    select
        versao_id,
        data_versao,
        data_extracao,
        endpoint_url,
        layer_id,
        layer_name,

        objectid,
        global_id,

        cap,
        ap,
        cnes,
        cod_equipe,
        cod_ine as ine_original,

        case
            when safe_cast(cod_ine as int64) = 0 then null
            else cod_ine
        end as ine,

        case
            when safe_cast(cod_ine as int64) = 0 then 'AREA_SEM_COBERTURA_ESF'
            else 'AREA_COM_COBERTURA_ESF'
        end as tipo_cobertura_esf,

        cod_area,
        nome_area,
        nome_fantasia as nome_unidade,
        tipo_unidade_aps,
        bairro,
        logradouro,
        numero,
        complemento,
        cod_cep,
        telefone,
        email,

        dt_ativa,
        tipo_eqp,

        geometry_corrigida,
        geometry,
        st_area(geometry) / 1000000 as area_km2,
        st_perimeter(geometry) / 1000 as perimetro_km,

        feature_hash,
        layer_hash,
        datalake_loaded_at
    from geometria
    where geometry is not null
)

select *
from tratado