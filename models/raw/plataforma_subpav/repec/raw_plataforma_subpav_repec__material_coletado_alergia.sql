{{
    config(
        alias="repec__material_coletado_alergia",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__material_coletado_alergia") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_alergia') }} as id_alergia,
            {{ process_null('id_paciente') }} as id_paciente,
            {{ process_null('dci_alergia') }} as dci_alergia,
            {{ process_null('material_alergia') }} as material_alergia,
            {{ process_null('data_inicio_alergia') }} as data_inicio_alergia,
            {{ process_null('data_registro') }} as data_registro,
            {{ process_null('origem_arquivo') }} as origem_arquivo,
            {{ process_null('origem_banco') }} as origem_banco,
            {{ repec_origem_unidade_para_cnes("origem_unidade") }} as cnes_origem,
            safe_cast({{ process_null('datalake_loaded_at') }} as timestamp) as datalake_loaded_at
        from source
    ),

    deduplicar as (
        select *
        from tratar_campos
        qualify row_number() over (
            partition by
                    id_alergia,
                    id_paciente,
                    dci_alergia,
                    material_alergia,
                    data_inicio_alergia,
                    data_registro
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
