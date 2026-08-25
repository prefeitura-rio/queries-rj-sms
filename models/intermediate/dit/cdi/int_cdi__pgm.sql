{{ 
    config(
        schema = "intermediario_cdi",
        alias  = "pgm",
        materialized = "table",
        meta={"owner": "karen"}
) }}

with pgm_2025 as (

    select
        processo_rio,
        procurador,
        requerente,
        processo_judicial,
        origem,
        data_entrada,
        data_saida,
        data_saida_pgm,
        prazo,
        mes_ano,
        sexo,
        idade,
        hospital_origem,
        cap,
        erro_medico,
        acp,
        tipo_indenizacao,
        valor,
        mandado_prisao,
        crime_desobediencia,
        patologia_assunto,
        solicitacao,
        sintese_solicitacao,
        setor_responsavel,
        prazo_dias,
        situacao,
        pendencias,
        observacoes

    from {{ ref('raw_cdi__pgm_2025') }}

),

pgm_2026 as (

    select
        processo_rio,
        procurador,
        requerente,
        processo_judicial,
        origem,
        data_entrada,
        data_saida,
        data_saida_pgm,
        prazo,
        mes_ano,
        sexo,
        idade,
        hospital_origem,
        cap,
        erro_medico,
        acp,
        tipo_indenizacao,
        valor,
        mandado_prisao,
        crime_desobediencia,
        patologia_assunto,
        solicitacao,
        sintese_solicitacao,
        setor_responsavel,
        prazo_dias,
        situacao,
        cast(null as string) as pendencias,
        observacoes

    from {{ ref('raw_cdi__pgm_2026') }}

),

base as (

    select * from pgm_2025

    union all

    select * from pgm_2026

)

select *
from base
where not (
    data_entrada is null
    and processo_judicial is null
    and processo_rio is null
)