{{
    config(
        alias="episodio_assistencial",
        schema="app_historico_clinico",
        materialized="table",
        cluster_by="cpf",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    paciente_restritos as (
        select cpf
        from {{ ref("mart_historico_clinico_app__paciente") }}
        where exibicao.indicador = false
    ),
    episodios_com_cid as (
        select id_episodio
        from {{ ref("mart_historico_clinico__episodio") }}, unnest(condicoes) as cid
        where cid.id is not null and cid.situacao <> 'RESOLVIDO'
    ),
    todos_episodios as (
        select
            *,
            case
                when paciente_cpf in (select cpf from paciente_restritos)
                then true
                else false
            end as flag__paciente_tem_restricao,
            case
                when paciente_cpf is null then true else false
            end as flag__paciente_sem_cpf,
            case
                when
                    tipo like '%Exame%' then false
                when 
                    tipo not like '%Exame%' and 
                    (
                        id_episodio in (select * from episodios_com_cid)
                        or motivo_atendimento is not null
                        or desfecho_atendimento is not null
                    )
                then false
                else true
            end as flag__episodio_sem_informacao
        from {{ ref("mart_historico_clinico__episodio") }}
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- FORMATAÇÃO
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    formatado as (
        select
            paciente_cpf as cpf,
            id_episodio,
            safe_cast(entrada_datahora as string) as entry_datetime,
            safe_cast(saida_datahora as string) as exit_datetime,
            safe_cast(estabelecimento.nome as string) as location,
            safe_cast(tipo as string) as type,
            safe_cast(subtipo as string) as subtype,
            safe_cast(
                case
                    when tipo = 'Exame' then 'clinical_exam' else 'default'
                end as string
            ) as exhibition_type,
            array(
                select struct(tipo as type, descricao as description)
                from unnest(exames_realizados)
                where tipo is not null
            ) as clinical_exams,
            array(
                select distinct descricao from unnest(condicoes) where descricao is not null and situacao <> 'RESOLVIDO'
            ) as active_cids,
            array(
                select distinct resumo from unnest(condicoes) where resumo is not null and resumo != ''
            ) as active_cids_summarized,
            case
                when
                    profissional_saude_responsavel.nome is not null
                    and profissional_saude_responsavel.especialidade is not null
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
                when estabelecimento.estabelecimento_tipo is null
                then []
                when
                    estabelecimento.estabelecimento_tipo
                    in ('CLINICA DA FAMILIA', 'CENTRO MUNICIPAL DE SAUDE')
                then ['CF/CMS']
                else array(select estabelecimento.estabelecimento_tipo)
            end as filter_tags,
            struct(
                not (
                    flag__episodio_sem_informacao
                    or flag__paciente_tem_restricao
                    or flag__paciente_sem_cpf
                ) as indicador,
                flag__episodio_sem_informacao as episodio_sem_informacao,
                flag__paciente_tem_restricao as paciente_restrito,
                flag__paciente_sem_cpf as paciente_sem_cpf
            ) as exibicao,
            prontuario.fornecedor as provider,
            cpf_particao
        from todos_episodios
    )
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- FINAL
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from formatado
where {{ validate_cpf("cpf") }}
