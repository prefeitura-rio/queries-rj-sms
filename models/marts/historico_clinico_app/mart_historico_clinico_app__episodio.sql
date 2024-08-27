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
    ),
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- REGRAS DE EXIBIÇÃO
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- Regra 1: Sem Identificador de Paciente
    regra_sem_identificador as (
        select 
            todos_episodios.id_atendimento,
            safe_cast(
                case
                    when paciente.cpf is null then true
                    else false
                end
            as boolean) as tem_exibicao_limitada,
            safe_cast(
                case
                    when paciente.cpf is null then "Paciente sem CPF"
                    else null
                end
            as string) as motivo
        from todos_episodios
    ),
    -- Regras 2: Sem Informações Básicas
    regra_sem_dados_basicos as (
        select 
            todos_episodios.id_atendimento,
            safe_cast(
                case
                    when 
                        todos_episodios.tipo != "Exame" and
                        array_length(todos_episodios.condicoes) = 0 and 
                        todos_episodios.motivo_atendimento is null and
                        todos_episodios.desfecho_atendimento is null
                        then true
                    else false
                end
            as boolean) as tem_exibicao_limitada,
            safe_cast(
                case
                    when 
                        todos_episodios.tipo != "Exame" and
                        array_length(todos_episodios.condicoes) = 0 and 
                        todos_episodios.motivo_atendimento is null and
                        todos_episodios.desfecho_atendimento is null
                        then "Episódio Não Informativo"
                    else null
                end
            as string) as motivo
        from todos_episodios
    ),
    -- Regra 3: Pacientes Não Restrito
    regra_paciente_restrito as (
        select
            todos_episodios.id_atendimento,
            todos_pacientes.exibicao.indicador as indicador,
            safe_cast(
                case
                    when todos_pacientes.exibicao.indicador = false then "Paciente Restrito"
                    else null
                end
            as string) as motivo
        from todos_episodios
            inner join {{ ref('mart_historico_clinico_app__paciente') }} as todos_pacientes
                on todos_episodios.paciente_cpf = todos_pacientes.cpf
    ),
    -- Juntando Regras
    todas_regras as (
        select * from regra_sem_identificador
        union all
        select * from regra_sem_dados_basicos
        union all
        select * from regra_paciente_restrito
    ),
    -- Agrupando Regras
    regras_exibicao as (
        select 
            id_atendimento,
            struct(
                not(logical_or(tem_exibicao_limitada)) as indicador,
                array_agg(motivo ignore nulls) as motivos
            ) as exibicao
        from todas_regras
        group by id_atendimento
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
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- JUNTANDO INFORMAÇÕES DE EXIBICAO
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select
    formatado.*,
    regras_exibicao.* except(id_atendimento)
from formatado
    left join regras_exibicao using (id_atendimento)