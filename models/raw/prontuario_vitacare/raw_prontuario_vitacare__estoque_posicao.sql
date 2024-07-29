{{
    config(
        alias="estoque_posicao",
        tags="vitacare_estoque"
    )
}}

with
    source as (
        select *
        from {{ source("brutos_prontuario_vitacare_staging", "estoque_posicao") }}
    ),
    renamed as (
        select
            {{ adapter.quote("ap") }} as area_programatica,
            {{ adapter.quote("cnesUnidade") }} as id_cnes,
            {{ adapter.quote("nomeUnidade") }} as estabelecimento_nome,
            {{ adapter.quote("desigMedicamento") }} as material_descricao,
            {{ adapter.quote("atc") }} as id_atc,
            {{ adapter.quote("code") }} as id_material,
            {{ adapter.quote("lote") }} id_lote,
            {{ adapter.quote("dtaCriLote") }} as lote_data_cadastro,
            {{ adapter.quote("dtaValidadeLote") }} as lote_data_vencimento,
            {{ adapter.quote("estoqueLote") }} as material_quantidade,
            {{ adapter.quote("id") }} as id_estoque_posicao,
            {{ adapter.quote("_data_carga") }} as data_carga,
            {{ adapter.quote("ano_particao") }} as ano_particao,
            {{ adapter.quote("mes_particao") }} as mes_particao,
            {{ adapter.quote("data_particao") }} as data_particao

        from source
    )
select
    -- Primary key
    safe_cast(id_estoque_posicao as string) as id_estoque_posicao,

    -- Foreign Keys
    safe_cast(area_programatica as string) as area_programatica,
    safe_cast(id_cnes as string) as id_cnes,
    safe_cast(id_lote as string) as id_lote,
    regexp_replace(
                safe_cast(id_material as string), r'[^a-zA-Z0-9]', ''
            ) as id_material,
    safe_cast(id_atc as string) as id_atc,

    -- Common Fields
    estabelecimento_nome,
    safe_cast(lote_data_cadastro as date) as lote_data_cadastro,
    safe_cast(lote_data_vencimento as date) as lote_data_vencimento,
    material_descricao,
    safe_cast(material_quantidade as float64) as material_quantidade,

    -- Metadata
    safe_cast(data_particao as date) as data_particao,
    safe_cast(data_carga as datetime) as data_carga

from renamed
where safe_cast(material_quantidade as float64) > 0