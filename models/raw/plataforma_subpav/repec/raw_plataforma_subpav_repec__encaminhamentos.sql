{{
    config(
        alias="repec__encaminhamentos",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__encaminhamentos") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_encaminhamento') }} as id_encaminhamento,
            {{ process_null('id_atendimento') }} as id_atendimento,
            {{ process_null('especialidade') }} as especialidade,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('tipo_encaminhamento') ~ " as string)") }}) as tipo_encaminhamento,
            {{ process_null('profissional_sugerido') }} as profissional_sugerido,
            {{ process_null('unidade_sugerida') }} as unidade_sugerida,
            {{ process_null('motivo_encaminhamento') }} as motivo_encaminhamento,
            {{ process_null('exames_solicitados') }} as exames_solicitados,
            {{ process_null('diagnostico_enc') }} as diagnostico_enc,
            {{ process_null('data_encaminhamento') }} as data_encaminhamento,
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
                    id_encaminhamento,
                    id_atendimento,
                    especialidade,
                    tipo_encaminhamento,
                    profissional_sugerido,
                    unidade_sugerida,
                    motivo_encaminhamento,
                    exames_solicitados,
                    diagnostico_enc,
                    data_encaminhamento
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
