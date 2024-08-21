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
    ),
    with_exhibition_configuration as (
        select 
            merged.*,
            struct(
                safe_cast(
                    if(DATE_DIFF('2020-03-21', paciente.dados.data_nascimento, YEAR) >= 18, true, false)                    
                as boolean) as indicador,
                safe_cast(
                    if(DATE_DIFF('2020-03-21', paciente.dados.data_nascimento, YEAR) >= 18, null, "Menor de Idade")
                as string) as motivo
            ) as registro_exibido
        from merged
            left join {{ ref("mart_historico_clinico__paciente") }} as paciente 
                on paciente.cpf = merged.paciente.cpf
    )
select *
from with_exhibition_configuration