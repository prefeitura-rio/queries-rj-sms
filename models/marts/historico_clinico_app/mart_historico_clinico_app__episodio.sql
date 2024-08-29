{{
    config(
        alias="episodio_assistencial",
        materialized="table",
        cluster_by="cpf",
        schema="app_historico_clinico",
    )
}}

with
    paciente_restritos as (
        select 
            cpf
        from {{ ref('mart_historico_clinico_app__paciente') }}
        where
            exibicao.indicador = false
    ),
    episodios_com_cid as (
        select
            id_atendimento
        from {{ ref('mart_historico_clinico__episodio') }}, unnest(condicoes) as cid
        where
            cid.id is not null
    ),
    todos_episodios as (
        select
            *,
            case
                when paciente_cpf in (select cpf from paciente_restritos) then true
                else false
            end as flag__paciente_tem_restricao,
            case
                when paciente_cpf is null then true
                else false
            end as flag__paciente_sem_cpf,
            case 
                when 
                    tipo like '%Exame%' or
                    tipo like '%Laborat%' or
                    tipo like '%Imagem%' or
                    id_atendimento in (select * from episodios_com_cid) or
                    motivo_atendimento is not null or
                    desfecho_atendimento is not null
                then false
                else true
            end as flag__episodio_sem_informacao
        from {{ ref('mart_historico_clinico__episodio') }}
    ),
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- FORMATAÇÃO
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    formatado as (
        select
            paciente_cpf as cpf,
            id_atendimento,
            safe_cast(entrada_datahora as string) as entry_datetime,
            safe_cast(saida_datahora as string) as exit_datetime,
            safe_cast(estabelecimento.nome as string) as location,
            safe_cast(tipo as string) as type,
            safe_cast(subtipo as string) as subtype,
            safe_cast(
                case 
                    when tipo = 'Exame' then 'clinical_exam'
                    else 'default'
                end
            as string) as exhibition_type,
            array(
                select
                    struct(
                        tipo as type,
                        descricao as description
                    )
                from unnest(exames_realizados) 
                where tipo is not null
            ) as clinical_exams,
            array(
                select descricao from unnest(condicoes) where descricao is not null
            ) as active_cids,
            case
                when 
                    profissional_saude_responsavel.nome is not null and
                    profissional_saude_responsavel.especialidade is not null
                then 
                    struct(
                        profissional_saude_responsavel.nome as name,
                        profissional_saude_responsavel.especialidade as role
                    )
                else null
            end as responsible,
            motivo_atendimento as clinical_motivation,
            desfecho_atendimento as clinical_outcome,
            case 
                when estabelecimento.estabelecimento_tipo is null then []
                when estabelecimento.estabelecimento_tipo in ('CLINICA DA FAMILIA','CENTRO MUNICIPAL DE SAUDE') then ['CF/CMS']
                else 
                    array(
                        select estabelecimento.estabelecimento_tipo
                    )
            end as filter_tags,
            struct(
                not(flag__episodio_sem_informacao or flag__paciente_tem_restricao or flag__paciente_sem_cpf) as indicador,
                flag__episodio_sem_informacao as episodio_sem_informacao,
                flag__paciente_tem_restricao as paciente_restrito,
                flag__paciente_sem_cpf as paciente_sem_cpf
            ) as exibicao
        from todos_episodios
    )
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- FINAL
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select
    *
from formatado