{{
    config(
        alias="saude_mental",
        schema="saude_historico_clinico",
        tags=["hci", "saude_mental"],
        materialized="table",
    )
}}

with 
    ciclos as (
        select
            id_paciente,
            ciclos_tratamento
        from {{ ref("saude_mental_ciclos_tratamento") }}
    ),

    matriciamento as (
        select
            id_paciente,
            mat.data_inicio,
            mat.tipo,
            mat.forma,
            mat.evolucao,
            mat.nome_unidade,
            mat.id_cnes
        from {{ ref("int_historico_clinico__matriciamento__pcsm") }} m 
            unnest(m.matriciamentos) as mat
    ),

    acolhimentos_single as (
        select
            cpf as paciente_cpf,
            acolhimento.nome_unidade,
            acolhimento.id_cnes,
            acolhimento.leito_ocupado, 
            acolhimento.datahora_entrada,
            acolhimento.datahora_saida,
        from {{ ref("int_historico_clinico__acolhimento__pcsm") }} a 
            unnest(a.acolhimento) as acolhimento
    ),
    
