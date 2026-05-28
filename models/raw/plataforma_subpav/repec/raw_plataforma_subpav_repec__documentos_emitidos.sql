{{
    config(
        alias="repec__documentos_emitidos",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__documentos_emitidos") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_doc_emitido') }} as id_doc_emitido,
            {{ process_null('id_paciente') }} as id_paciente,
            {{ process_null('id_atendimento') }} as id_atendimento,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('id_tipo_doc') ~ " as string)") }}) as id_tipo_doc,
            {{ process_null('path_documento') }} as path_documento,
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
                    id_doc_emitido,
                    id_paciente,
                    id_atendimento,
                    id_tipo_doc,
                    path_documento
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
