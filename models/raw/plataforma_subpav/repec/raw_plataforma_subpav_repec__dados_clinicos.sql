{{
    config(
        alias="repec__dados_clinicos",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__dados_clinicos") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_dado_clinico') }} as id_dado_clinico,
            {{ process_null('id_atendimento') }} as id_atendimento,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('id_tipo_dado_clinico') ~ " as string)") }}) as id_tipo_dado_clinico,
            {{ process_null('id_campo_dado_clinico') }} as id_campo_dado_clinico,
            {{ process_null('valor_num') }} as valor_num,
            {{ process_null('valor_desc') }} as valor_desc,
            {{ process_null('valor_sn') }} as valor_sn,
            {{ process_null('valor_data') }} as valor_data,
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
                    id_dado_clinico,
                    id_atendimento,
                    id_tipo_dado_clinico,
                    id_campo_dado_clinico,
                    valor_num,
                    valor_desc,
                    valor_sn,
                    valor_data
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
