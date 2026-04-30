{{
    config(
        alias="repec__vd_familia",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__vd_familia") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_vd_familia') }} as id_vd_familia,
            {{ process_null('cnes_entidade') }} as cnes_entidade,
            {{ process_null('id_familia') }} as id_familia,
            {{ process_null('id_profissional') }} as id_profissional,
            {{ process_null('data_atendimento') }} as data_atendimento,
            {{ process_null('data_registro') }} as data_registro,
            {{ process_null('disponibilidade') }} as disponibilidade,
            {{ process_null('num_kits_odonto') }} as num_kits_odonto,
            {{ process_null('risco_presente') }} as risco_presente,
            {{ process_null('acomp_orientacao') }} as acomp_orientacao,
            {{ process_null('observacoes_gerais') }} as observacoes_gerais,
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
                    id_vd_familia,
                    cnes_entidade,
                    id_familia,
                    id_profissional,
                    data_atendimento,
                    data_registro,
                    disponibilidade,
                    num_kits_odonto,
                    risco_presente,
                    acomp_orientacao,
                    observacoes_gerais
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
