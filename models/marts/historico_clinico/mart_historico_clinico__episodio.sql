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
    with_fingerprint as (
        select 
            farm_fingerprint(concat(prontuario.fornecedor, prontuario.id_atendimento)) as id_atendimento,
            merged.*,
        from merged
    ),
    with_exhibition_configuration as (
        select 
            with_fingerprint.*,
            struct(
                safe_cast(
                    case
                        when paciente.data_nascimento is null then true
                        when DATE_DIFF(current_date(), paciente.data_nascimento, YEAR) >= 18 then true
                        when DATE_DIFF(current_date(), paciente.data_nascimento, YEAR) < 18 then false
                    end
                as boolean) as indicador,
                safe_cast(
                    case
                        when paciente.data_nascimento is null then null
                        when DATE_DIFF(current_date(), paciente.data_nascimento, YEAR) >= 18 then null
                        when DATE_DIFF(current_date(), paciente.data_nascimento, YEAR) < 18 then "Menor de Idade"
                    end
                as string) as motivo
            ) as registro_exibido
        from with_fingerprint
    )
select *
from with_exhibition_configuration