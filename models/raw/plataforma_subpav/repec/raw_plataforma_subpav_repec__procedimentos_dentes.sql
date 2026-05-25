{{
    config(
        alias="repec__procedimentos_dentes",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__procedimentos_dentes") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_proc_dentes') }} as id_proc_dentes,
            {{ process_null('id_atendimento') }} as id_atendimento,
            {{ process_null('id_dente') }} as id_dente,
            {{ process_null('cod_procedimento') }} as cod_procedimento,
            {{ process_null('quant') }} as quant,
            {{ process_null('face_restauracao') }} as face_restauracao,
            {{ process_null('detalhe_restauracao') }} as detalhe_restauracao,
            {{ process_null('detalhe_capeamento') }} as detalhe_capeamento,
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
                    id_proc_dentes,
                    id_atendimento,
                    id_dente,
                    cod_procedimento,
                    quant,
                    face_restauracao,
                    detalhe_restauracao,
                    detalhe_capeamento
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
