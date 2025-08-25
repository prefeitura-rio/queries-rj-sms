{{ config(materialized="table", schema="projeto_c34", alias="producoes_sia") }}

with
    sia as (
        select distinct
            to_hex(
                sha256(cast(safe_cast(paciente_cpf as int64) as string))
            ) as paciente_id,
            safe_cast(paciente_cpf as int64) as paciente_cpf,
            safe_cast(paciente_cns as int64) as paciente_cns,

            "SIA" as sistema,

            procedimento_id,
            data_realizacao,
            unidade_executante_cnes,
            procedimento_qtd,
            data_inicial_apac,
            competencia_realizacao_proced,
            cid_princinpal
        from {{ source("sub_geral_prod", "c34_sia_2024") }}
    ),

    cns as (
        select cns_id, cns_item
        from {{ref("raw_projeto_c34__cns_fuzzy_match")}}
        left join unnest(cns_array) as cns_item
    ),

    consolidado as (
        select *
        from sia
        where paciente_cns in (select cns_item from cns)
    )

select * from consolidado 
-- 151 cns unicos
-- 2673 registros
-- aumentar pra 2023?
