{{
    config(
        alias="repec__tabaux_dose",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__tabaux_dose") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_dose_ident') }} as id_dose_ident,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('descricao') ~ " as string)") }}) as descricao,
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
                    id_dose_ident,
                    descricao
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
