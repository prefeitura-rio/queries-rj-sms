{{
    config(
        schema="saude_historico_clinico",
        alias="episodio_assistencial",
        materialized="table",
        cluster_by = "paciente_cpf",
    )
}}


with 
    vitai as (
        select * from {{ ref("int_historico_clinico__episodio__vitai") }}
    ),
    vitacare as (
        select * from {{ ref("int_historico_clinico__episodio__vitacare") }}
    ),
    merged as (
        select
            paciente.cpf as paciente_cpf,
            paciente, 
            tipo,
            subtipo,
            entrada_datahora,
            saida_datahora,
            motivo_atendimento,
            desfecho_atendimento,
            condicoes,
            null as prescricoes,
            estabelecimento, 
            profissional_saude_responsavel,
            prontuario,
            metadados
        from vitai

        union all

        select 
            paciente.cpf as paciente_cpf,
            paciente, 
            tipo,
            subtipo,
            entrada_datahora,
            saida_datahora,
            motivo_atendimento,
            desfecho_atendimento,
            condicoes,
            prescricoes,
            estabelecimento, 
            profissional_saude_responsavel,
            prontuario,
            metadados
        from vitacare
    )
select *
from merged