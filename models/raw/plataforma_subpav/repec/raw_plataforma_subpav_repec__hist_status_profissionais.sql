{{
    config(
        alias="repec__hist_status_profissionais",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__hist_status_profissionais") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_hist_status_profissional') }} as id_hist_status_profissional,
            {{ process_null('id_profissional') }} as id_profissional,
            {{ process_null('estado_profissional') }} as estado_profissional,
            {{ process_null('equipe_profissional') }} as equipe_profissional,
            {{ process_null('microarea_profissional') }} as microarea_profissional,
            {{ process_null('data_registro_profissional') }} as data_registro_profissional,
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
                    id_hist_status_profissional,
                    id_profissional,
                    estado_profissional,
                    equipe_profissional,
                    microarea_profissional,
                    data_registro_profissional
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
