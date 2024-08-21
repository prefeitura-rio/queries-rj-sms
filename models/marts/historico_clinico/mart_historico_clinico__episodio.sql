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
                    case
                        when paciente.dados.data_nascimento is null then true
                        when DATE_DIFF(current_date(), paciente.dados.data_nascimento, YEAR) >= 18 then true
                        when DATE_DIFF(current_date(), paciente.dados.data_nascimento, YEAR) < 18 then false
                    end
                as boolean) as indicador,
                safe_cast(
                    case
                        when paciente.dados.data_nascimento is null then null
                        when DATE_DIFF(current_date(), paciente.dados.data_nascimento, YEAR) >= 18 then null
                        when DATE_DIFF(current_date(), paciente.dados.data_nascimento, YEAR) < 18 then "Menor de Idade"
                    end
                as string) as motivo
            ) as registro_exibido
        from merged
            left join {{ ref("mart_historico_clinico__paciente") }} as paciente 
                on paciente.cpf = merged.paciente.cpf
    )
select *
from with_exhibition_configuration