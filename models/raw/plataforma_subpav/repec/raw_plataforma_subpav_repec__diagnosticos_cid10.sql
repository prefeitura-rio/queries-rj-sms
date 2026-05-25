{{
    config(
        alias="repec__diagnosticos_cid10",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__diagnosticos_cid10") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_diagnostico') }} as id_diagnostico,
            {{ process_null('id_atendimento') }} as id_atendimento,
            {{ process_null('cod_cid10') }} as cod_cid10,
            {{ process_null('estado') }} as estado,
            {{ process_null('data_ativo') }} as data_ativo,
            {{ process_null('data_resolvido') }} as data_resolvido,
            {{ process_null('data_ne') }} as data_ne,
            {{ process_null('observacoes') }} as observacoes,
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
                    id_diagnostico,
                    id_atendimento,
                    cod_cid10,
                    estado,
                    data_ativo,
                    data_resolvido,
                    data_ne,
                    observacoes,
                    data_registro
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
