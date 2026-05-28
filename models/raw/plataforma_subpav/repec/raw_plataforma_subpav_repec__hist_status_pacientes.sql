{{
    config(
        alias="repec__hist_status_pacientes",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__hist_status_pacientes") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_hist_status_paciente') }} as id_hist_status_paciente,
            {{ process_null('id_paciente') }} as id_paciente,
            {{ process_null('id_familia') }} as id_familia,
            {{ process_null('equipe_paciente') }} as equipe_paciente,
            {{ process_null('situacao_paciente') }} as situacao_paciente,
            {{ process_null('id_profissional') }} as id_profissional,
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
                    id_hist_status_paciente,
                    id_paciente,
                    id_familia,
                    equipe_paciente,
                    situacao_paciente,
                    id_profissional,
                    data_registro
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
