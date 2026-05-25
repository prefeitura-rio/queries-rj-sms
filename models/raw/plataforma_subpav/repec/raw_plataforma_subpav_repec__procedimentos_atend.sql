{{
    config(
        alias="repec__procedimentos_atend",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__procedimentos_atend") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_proc_atd') }} as id_proc_atd,
            {{ process_null('id_atendimento') }} as id_atendimento,
            {{ process_null('cod_procedimento') }} as cod_procedimento,
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
                    id_proc_atd,
                    id_atendimento,
                    cod_procedimento
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
