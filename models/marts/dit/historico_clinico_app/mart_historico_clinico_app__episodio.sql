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
        select id_hci
        from {{ ref("mart_historico_clinico__episodio") }}, unnest(condicoes) as cid
        where cid.id is not null
    ),
    episodios_com_procedimento as (
        select id_hci
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
                        id_hci in (select * from episodios_com_cid)
                        or id_hci in (select * from episodios_com_procedimento)
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
    encounter_medicines as (
        select distinct 
        ep.id_hci, 
        concat(med.nome,-- lala, , s , 
                ', ',
                IF(med.unidade_medida is null, '',med.unidade_medida),
                ', ',
                IF(med.uso is null,'',med.uso),
                ', ',
                IF(med.via_administracao is null,'',med.via_administracao),
                IF(med.quantidade is null,'',concat('. QUANTIDADE: ',med.quantidade))
            ) as medicamento_administrado,
        med.prescricao_data
        from {{ ref("mart_historico_clinico__episodio") }} as ep, unnest(ep.medicamentos_administrados) as med
        where med.nome is not null
    ),
    encounter_medicines_agg as (
        select
            id_hci,
            array_agg( 
                struct(
                    regexp_replace(medicamento_administrado,'(, )+',', ') as name,
                    prescricao_data as prescription_date
                )
                order by prescricao_data desc
            ) as medicines_administered
        from encounter_medicines
        group by 1
    ),
    encounter_prescription as (
        select 
        distinct ep.id_hci, 
        concat(p.nome,' ',p.concentracao) as prescricao
        from {{ ref("mart_historico_clinico__episodio") }} as ep, unnest(ep.prescricoes) as p
    ),
    encounter_prescription_agg as (
        select 
        id_hci, 
        string_agg(prescricao,'\n') as prescription
        from encounter_prescription
        group by 1
    ),

    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- FORMATAÇÃO
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    formatado as (
        select
            todos_episodios.id_hci,
            paciente_cpf as cpf,
            safe_cast(coalesce(entrada_datahora,saida_datahora) as string) as entry_datetime,
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
            safe_cast(procedimentos_realizados as string) as procedures,
            struct(
                medidas.altura as height,
                medidas.circunferencia_abdominal as abdominal_circumference,
                medidas.frequencia_cardiaca as heart_rate,
                medidas.frequencia_respiratoria as respiratory_rate,
                medidas.glicemia as blood_glucose,
                medidas.hemoglobina_glicada as glycated_hemoglobin,
                medidas.imc as bmi,
                medidas.peso as weight,
                medidas.pressao_sistolica as systolic_pressure,
                medidas.pressao_diastolica as diastolic_pressure,
                medidas.pulso_ritmo as pulse_rate,
                medidas.saturacao_oxigenio as oxygen_saturation,
                medidas.temperatura as temperature
            ) as measures,
            safe_cast(prescription as string) as prescription,
            medicines_administered,
            array(
                select struct(descricao as description , situacao as status) 
                from unnest(condicoes) 
                where descricao is not null
            ) as cids,
            array(
                select distinct resumo
                from unnest(condicoes)
                where resumo is not null and resumo != ''
            ) as cids_summarized,
            case
                when
                    profissional_saude_responsavel.nome is not null
                    and profissional_saude_responsavel.especialidade is not null
                then
                    struct(
                        profissional_saude_responsavel.nome as name,
                        {{
                            capitalize_first_letter("
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                        lower(profissional_saude_responsavel.especialidade),
                                        ' da estrat[eé]gia( de){0,1} sa[uú]de da fam[ií]lia',
                                        ''
                                        ),
                                        r'\(.*\)',
                                        ''
                                    ),
                                    r'(modelo b)|( \-.*)|( para pacientes com necessidades especiais)|( nasf)',
                                    ''
                                    )
                            ")
                        }} as role
                    )
                else null
            end as responsible,
            motivo_atendimento as clinical_motivation,
            desfecho_atendimento as clinical_outcome,
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
        left join encounter_prescription_agg
        on todos_episodios.id_hci = encounter_prescription_agg.id_hci
        left join encounter_medicines_agg
        on todos_episodios.id_hci = encounter_medicines_agg.id_hci
    )
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- FINAL
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from formatado
where {{ validate_cpf("cpf") }}
