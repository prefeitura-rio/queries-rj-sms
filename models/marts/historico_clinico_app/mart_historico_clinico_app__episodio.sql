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
                select descricao from unnest(condicoes) where descricao is not null
            ) as active_cids,
            struct(
                profissional_saude_responsavel[safe_offset(0)].nome as name,
                profissional_saude_responsavel[safe_offset(0)].especialidade as role
            ) as responsible,
            motivo_atendimento as clinical_motivation,
            desfecho_atendimento as clinical_outcome,
            case 
                when estabelecimento.estabelecimento_tipo is null then []
                when estabelecimento.estabelecimento_tipo in ('CLINICA DA FAMILIA','CENTRO MUNICIPAL DE SAUDE') then ['CF/CMS']
                else 
                    array(
                        select estabelecimento.estabelecimento_tipo
                    )
            end as filter_tags
        from todos_episodios
    )

select
    *
from formatado