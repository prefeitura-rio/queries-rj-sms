{{
    config(
        alias="repec__vacinas",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__vacinas") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_vacina_paciente') }} as id_vacina_paciente,
            {{ process_null('id_paciente') }} as id_paciente,
            {{ process_null('id_profissional') }} as id_profissional,
            {{ process_null('id_vacina') }} as id_vacina,
            {{ process_null('dose_vacina') }} as dose_vacina,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('tipo_registro') ~ " as string)") }}) as tipo_registro,
            {{ process_null('aplicavel') }} as aplicavel,
            {{ process_null('lote_vacina') }} as lote_vacina,
            {{ process_null('data_validade_vacina') }} as data_validade_vacina,
            {{ process_null('laboratorio_vacina') }} as laboratorio_vacina,
            {{ process_null('local_inoculacao') }} as local_inoculacao,
            {{ process_null('data_administracao') }} as data_administracao,
            {{ process_null('obs_vacina') }} as obs_vacina,
            {{ process_null('data_registro_vacina') }} as data_registro_vacina,
            {{ process_null('efeitos_adv_vacina') }} as efeitos_adv_vacina,
            {{ process_null('data_registro_efeitos_adv') }} as data_registro_efeitos_adv,
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
                    id_vacina_paciente,
                    id_paciente,
                    id_profissional,
                    id_vacina,
                    dose_vacina,
                    tipo_registro,
                    aplicavel,
                    lote_vacina,
                    data_validade_vacina,
                    laboratorio_vacina,
                    local_inoculacao,
                    data_administracao,
                    obs_vacina,
                    data_registro_vacina,
                    efeitos_adv_vacina,
                    data_registro_efeitos_adv
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
