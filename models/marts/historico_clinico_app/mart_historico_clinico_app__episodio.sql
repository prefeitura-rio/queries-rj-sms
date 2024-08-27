{{
    config(
        alias="episodio_assistencial",
        materialized="table",
        cluster_by="cpf",
        schema="app_historico_clinico",
    )
}}

with
    todos_episodios as (
        select
            *
        from {{ ref('mart_historico_clinico__episodio') }}
        where paciente_cpf is not null
    ),
    formatado as (
        select
            paciente_cpf as cpf,
            entrada_datahora as entry_datetime,
            saida_datahora as exit_datetime,
            estabelecimento.nome as location,
            tipo as type,
            subtipo as subtype,
            array(
                select descricao
                from unnest(condicoes)
                where descricao is not null
            ) as active_cids,
            profissional_saude_responsavel[safe_offset(0)] as responsible,
            motivo_atendimento as clinical_motivation,
            desfecho_atendimento as clinical_outcome,
            case 
                when estabelecimento.estabelecimento_tipo is null then []
                when estabelecimento.estabelecimento_tipo in ('CLINICA DA FAMILIA','CENTRO MUNICIPAL DE SAUDE') then ['CF/CMS']
                else array(
                select estabelecimento.estabelecimento_tipo
                )
            end as filter_tags
        from todos_episodios
    )

select
    *
from formatado