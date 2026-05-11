{{
    config(
        alias = "arcgis__osa_cadastros_vinculados_area",
        materialized = "table",
        tags = ["subpav", "arcgis", "hci", "onde_ser_atendido", "sobrecarga"]
    )
}}

with areas_feicoes as (
    select
        versao_id,
        layer_hash,
        data_versao,
        data_extracao,

        objectid,
        ap,
        cap,
        cnes,
        ine,
        ine_original,
        tipo_cobertura_esf,
        cod_equipe,
        cod_area,

        nome_area,
        nome_unidade,
        tipo_unidade_aps,
        bairro,

        geometry_corrigida,
        geometry
    from {{ ref("int_arcgis__osa_equipes_territorio") }}
),

areas_agregadas as (
    select
        versao_id,
        layer_hash,
        data_versao,
        data_extracao,

        case
            when tipo_cobertura_esf = 'AREA_COM_COBERTURA_ESF'
                then concat(cnes, '|', ine)
            else concat('SEM_COBERTURA|', cast(objectid as string))
        end as chave_area_territorial,

        array_agg(objectid order by objectid) as objectids,
        count(*) as total_poligonos,

        array_agg(ap order by objectid limit 1)[offset(0)] as ap,
        array_agg(cap order by objectid limit 1)[offset(0)] as cap,
        cnes,
        ine,
        ine_original,
        tipo_cobertura_esf,

        string_agg(
            distinct cast(cod_equipe as string),
            ' | '
            order by cast(cod_equipe as string)
        ) as cod_equipe,

        string_agg(
            distinct cast(cod_area as string),
            ' | '
            order by cast(cod_area as string)
        ) as cod_area,

        string_agg(
            distinct cast(nome_area as string),
            ' | '
            order by cast(nome_area as string)
        ) as nome_area,

        string_agg(
            distinct cast(nome_unidade as string),
            ' | '
            order by cast(nome_unidade as string)
        ) as nome_unidade,

        string_agg(
            distinct cast(tipo_unidade_aps as string),
            ' | '
            order by cast(tipo_unidade_aps as string)
        ) as tipo_unidade_aps,

        string_agg(
            distinct cast(bairro as string),
            ' | '
            order by cast(bairro as string)
        ) as bairro,

        logical_or(geometry_corrigida) as geometry_corrigida,
        st_union_agg(geometry) as geometry

    from areas_feicoes
    group by
        versao_id,
        layer_hash,
        data_versao,
        data_extracao,
        chave_area_territorial,
        cnes,
        ine,
        ine_original,
        tipo_cobertura_esf
),

areas as (
    select
        *,
        st_area(geometry) / 1000000 as area_km2,
        st_perimeter(geometry) / 1000 as perimetro_km
    from areas_agregadas
),

vinculos as (
    select
        cpf,
        cnes_vinculo,
        ine_vinculo
    from {{ ref("int_arcgis__hci_paciente_vinculo_equipe") }}
    where cnes_vinculo is not null
        and ine_vinculo is not null
),

agregado_sem_geometry as (
    select
        areas.versao_id,
        areas.layer_hash,
        areas.data_versao,
        areas.data_extracao,

        areas.chave_area_territorial,
        areas.objectids,
        areas.total_poligonos,

        areas.ap,
        areas.cap,
        areas.cnes,
        areas.ine,
        areas.ine_original,
        areas.tipo_cobertura_esf,
        areas.cod_equipe,
        areas.cod_area,

        areas.nome_area,
        areas.nome_unidade,
        areas.tipo_unidade_aps,
        areas.bairro,

        areas.area_km2,
        areas.perimetro_km,
        areas.geometry_corrigida,

        count(distinct vinculos.cpf) as qtd_cadastrados_vinculados,

        3500 as parametro_municipal_cadastros_equipe,

        count(distinct vinculos.cpf) - 3500
            as diferenca_parametro_municipal,

        safe_divide(
            count(distinct vinculos.cpf),
            3500
        ) as razao_parametro_municipal,

        safe_divide(
            count(distinct vinculos.cpf),
            nullif(areas.area_km2, 0)
        ) as densidade_cadastrados_vinculados_km2,

        case
            when areas.tipo_cobertura_esf = 'AREA_SEM_COBERTURA_ESF'
                then 'AREA_SEM_COBERTURA_ESF'
            when count(distinct vinculos.cpf) > 3500
                then 'ACIMA_PARAMETRO_MUNICIPAL'
            when count(distinct vinculos.cpf) = 3500
                then 'NO_LIMITE_PARAMETRO_MUNICIPAL'
            when count(distinct vinculos.cpf) > 0
                then 'DENTRO_PARAMETRO_MUNICIPAL'
            else 'SEM_CADASTROS_VINCULADOS'
        end as classificacao_carga_vinculada

    from areas
    left join vinculos
        on areas.cnes = vinculos.cnes_vinculo
        and areas.ine = vinculos.ine_vinculo

    group by
        areas.versao_id,
        areas.layer_hash,
        areas.data_versao,
        areas.data_extracao,
        areas.chave_area_territorial,
        areas.objectids,
        areas.total_poligonos,
        areas.ap,
        areas.cap,
        areas.cnes,
        areas.ine,
        areas.ine_original,
        areas.tipo_cobertura_esf,
        areas.cod_equipe,
        areas.cod_area,
        areas.nome_area,
        areas.nome_unidade,
        areas.tipo_unidade_aps,
        areas.bairro,
        areas.area_km2,
        areas.perimetro_km,
        areas.geometry_corrigida
),

final as (
    select
        agregado_sem_geometry.*,
        areas.geometry
    from agregado_sem_geometry
    left join areas
        on agregado_sem_geometry.versao_id = areas.versao_id
        and agregado_sem_geometry.layer_hash = areas.layer_hash
        and agregado_sem_geometry.chave_area_territorial = areas.chave_area_territorial
)

select *
from final