{{
    config(
        alias="saude_episodio_assistencial",
        schema="app_identidade_unica",
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
        where cid.id is not null
    ),
    episodios_com_procedimento as (
        select id_episodio
        from {{ ref("mart_historico_clinico__episodio") }}
        where procedimentos_realizados is not null
    ),
    todos_episodios as (
        select
            *,
            -- Flag de Paciente com Restrição
            case
                when paciente_cpf in (select cpf from paciente_restritos)
                then true
                else false
            end as flag__paciente_tem_restricao,

            -- Flag de Paciente sem CPF
            case
                when paciente_cpf is null then true else false
            end as flag__paciente_sem_cpf,

            -- Flag de Exame sem Subtipo
            case
                when tipo = 'Exame' and subtipo is null then true else false
            end as flag__exame_sem_subtipo,

            -- Flag de Episódio de Vacinação
            case
                when tipo = 'Vacinação' then true else false
            end as flag__episodio_vacinacao,

            -- Flag de Episódio não informativo
            case
                when tipo like '%Exame%'
                then false
                when
                    tipo not like '%Exame%'
                    and (
                        id_episodio in (select * from episodios_com_cid)
                        or id_episodio in (select * from episodios_com_procedimento)
                        or motivo_atendimento is not null
                        or desfecho_atendimento is not null
                    )
                then false
                else true
            end as flag__episodio_sem_informacao,

            -- Flag de Subtipo Proibido
            case
                when
                    prontuario.fornecedor = 'vitacare'
                    and subtipo in (
                        'Consulta de Fisioterapia',
                        'Consulta de Assistente Social',
                        'Atendimento de Nutrição NASF',
                        'Ficha da Aula',
                        'Consulta de Atendimento Farmacêutico',
                        'Consulta de Fonoaudiologia',
                        'Consulta de Terapia Ocupacional',
                        'Gestão de arquivo não médico',
                        'Gestão de Arquivo Assistente Social NASF',
                        'Gestão de Arquivo de Professor NASF',
                        'Gestão de Arquivo Não Médico NASF',
                        'Gestão de Arquivo Fisioterapeuta NASF',
                        'Atendimento de Nutrição Modelo B',
                        'Gestão de Arquivo Não Médico',
                        'Gestão de Arquivo Fonoaudiólogo NASF',
                        'Atendimento de Fisioterapia Modelo B',
                        'Atendimento de Fonoaudiologia Modelo B',
                        'Atendimento de Assistente Social Modelo B',
                        'Gestão de Arquivo Farmacêutico NASF',
                        'Gestão de Arquivo de Terapeuta Ocupacional NASF',
                        'Consulta de Acupuntura',
                        'Ato Gestão de Arquivo não Médico',
                        'Gestão de Arquivo não Médico',
                        'Atendimento Nutricionismo'
                    )
                then true
                else false
            end as flag__subtipo_proibido_vitacare

        from {{ ref("mart_historico_clinico__episodio") }}
    ),


    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- FORMATAÇÃO
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    formatado as (
        select
            paciente_cpf as cpf,
            todos_episodios.id_episodio,
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
            
            obito_indicador as deceased,
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
                    or flag__subtipo_proibido_vitacare
                    or flag__episodio_vacinacao
                    or flag__exame_sem_subtipo
                ) as indicador,
                flag__episodio_sem_informacao as episodio_sem_informacao,
                flag__paciente_tem_restricao as paciente_restrito,
                flag__paciente_sem_cpf as paciente_sem_cpf,
                flag__subtipo_proibido_vitacare as subtipo_proibido_vitacare,
                flag__episodio_vacinacao as episodio_vacinacao,
                flag__exame_sem_subtipo as exame_sem_subtipo
            ) as exibicao,
            prontuario.fornecedor as provider,
            safe_cast(paciente_cpf as int64) as cpf_particao
        from todos_episodios
    )
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- FINAL
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from formatado
where {{ validate_cpf("cpf") }}
