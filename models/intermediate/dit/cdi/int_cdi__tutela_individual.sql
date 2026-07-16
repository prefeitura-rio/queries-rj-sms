{{
    config(
        schema = "intermediario_cdi",
        alias = "tutela_individual",
        materialized = "table",
        meta={"owner": "karen"}
    )
}}

with tutela_individual_2025 as (

    select
        mes,
        data_entrada,
        processo_rio,
        no_oficio,
        data_oficio,
        orgao,
        procedimento,
        promotor_defensor,
        objeto,
        assuntos,
        reiteracoes,
        area,
        sexo,
        idade,
        prazo_dias,
        vencimento,
        data_saida,
        orgao_para_subsidiar,
        no_oficio_sms,
        data_sms_ofi as data_finalizacao,
        observacoes,
        situacao

    from {{ ref('raw_cdi__tutela_individual_2025') }}

),

tutela_individual_2026 as (

    select
        mes,
        data_entrada,
        processo_rio,
        no_oficio,
        data_oficio,
        orgao,
        procedimento,
        promotor_defensor,
        objeto,
        assuntos,
        reiteracoes,
        area,
        sexo,
        idade,
        prazo_dias,
        vencimento,
        data_saida,
        orgao_para_subsidiar,
        no_oficio_sms,
        data_arquivamento as data_finalizacao,
        observacoes,
        situacao

    from {{ ref('raw_cdi__tutela_individual_2026') }}

),

base as (

    select * from tutela_individual_2025

    union all

    select * from tutela_individual_2026

),

calc as (

    select
        *,

        case
            when idade is null then null
            when regexp_contains(lower(idade), r'idos') then 'idoso'
            when regexp_contains(lower(idade), r'crian|infant|adolesc') then 'crianca_adolescente'
            when regexp_contains(lower(idade), r'adult') or lower(idade) = 'adulo' then 'adulto'
            when regexp_contains(lower(idade), r'n[uú]cleo\s+familiar') then 'nucleo_familiar'
            when regexp_contains(lower(idade), r'n[aã]o\s+identif') then 'nao_identificado'
            else 'ignorado'
        end as idade_categoria

    from base

)

select *
from calc
where processo_rio is not null
  and data_entrada is not null