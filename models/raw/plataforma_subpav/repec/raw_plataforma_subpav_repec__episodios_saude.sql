{{
    config(
        alias="repec__episodios_saude",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__episodios_saude") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_episodio_saude') }} as id_episodio_saude,
            {{ process_null('id_paciente') }} as id_paciente,
            {{ process_null('prog_saude') }} as prog_saude,
            {{ process_null('data_inicio') }} as data_inicio,
            {{ process_null('data_fim') }} as data_fim,
            {{ process_null('data_inicio_vigilancia') }} as data_inicio_vigilancia,
            {{ process_null('data_fim_vigilancia') }} as data_fim_vigilancia,
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
                    id_episodio_saude,
                    id_paciente,
                    prog_saude,
                    data_inicio,
                    data_fim,
                    data_inicio_vigilancia,
                    data_fim_vigilancia
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
