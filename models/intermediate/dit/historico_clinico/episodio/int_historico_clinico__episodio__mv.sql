{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_mv",
        materialized="table",
        unique_key=["id_hci"],
        cluster_by=["id_hci"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

/*
  O MV tem 4 tipos de atendimentos: Ambulatorial, Internação, Urgência e Externo.
  O tipo é definido a partir do campo atendimento_tipo. O fluxo do paciente é definido da seguinte forma:

  - Urgência:
  `atendimento`-> `bam` -> `parecer` (opcional) 

  - Internação:
  `atendimento`-> `evolucao` -> `parecer` (opcional) -> `alta`

  - Ambulatorial:
  `atendimento`-> `anamnese` -> `parecer` (opcional)

  O tipo de atendimento externo é ignorado por não se encaixar na definição de episódio assistencial, uma vez que 
  não há continuidade assistencial. Tendo em vista que um atendimento externo normalmente são realizados exames ou 
  procedimentos sem que haja uma consulta médica, o que dificulta a definição de um motivo de atendimento e desfecho, 
  além de não haver continuidade assistencial.

  Os registros são enviados de forma contínua, ou seja, a cada nova atualização dos dados, o MV é atualizado com os 
  novos registros e os registros já existentes são atualizados caso haja alguma mudança nos dados. Para garantir que 
  tenhamos os dados completos de um episódio assistencial, só consideramos os atendimentos que tiveram alta ou óbito, 
  ou seja, que estão completos.
*/
with

    atendimento as (
        select
            id_hci,
            id_atendimento,
            id_cnes,
            paciente_cpf,
            atendimento_datahora,
            alta_datahora,
            atendimento_tipo,
            atendimento_especialidade,
            atendimento_desfecho,
            id_cid,
            loaded_at,
            updated_at
        from {{ ref("raw_prontuario_mv__atendimento") }}
        where
            -- Data quando passamos a receber os dados completos (sem lacunas)
            atendimento_datahora > date('2026-06-02')
            and alta_datahora is not null
            and atendimento_tipo != 'EXTERNO'
    ),

    -- BAM
    bam as (
        select
            id_atendimento,
            atendimento_datahora,
            paciente_alergia,
            queixa_principal,
            queixa_medica,
            historia_doenca_atual,
            pressao_arterial_sistolica,
            pressao_arterial_diastolica,
            frequencia_cardiaca,
            frequencia_respiratoria,
            temperatura,
            saturacao_oxigenio,
            destino_paciente,
            hipotese_diagnostica,
            conduta_proposta,
            concat(
                upper(queixa_principal),
                '\n',
                upper(queixa_medica),
                '\n',
                upper(historia_doenca_atual)
            ) as motivo_atendimento,
            profissional_nome
        from {{ ref("raw_prontuario_mv__bam") }}
    ),

    -- Anamnese 
    anamnese as (
        select
            id_atendimento,
            queixa_principal,
            historia_pregressa,
            diagnostico_provavel,
            plano_terapeutico,
            historia_doenca_atual,
            conduta_proposta,
            pressao_arterial_sistolica,
            pressao_arterial_diastolica,
            frequencia_cardiaca,
            frequencia_respiratoria,
            temperatura,
            saturacao_oxigenio,
            peso,
            superficie_corporal,
            altura,
            imc,
            cid,
            profissional_nome,
            historia_doenca_atual,
            concat(
                upper(queixa_principal), '\n', upper(historia_doenca_atual)
            ) as motivo_atendimento,
            destino_paciente
        from {{ ref("raw_prontuario_mv__anamnese") }}
    ),

    parecer as (
        select
            id_atendimento,
            atendimento_datahora as data_diagnostico,
            split(cid, ' ')[0] as id_cid,
        from {{ ref("raw_prontuario_mv__parecer") }}
    ),

    -- Evolução
    evolucao as (
        select
            id_atendimento,
            profissional_nome,
            planejamento_terapeutico,
            resumo_internacao,
            conduta_adotada,
            diagnostico_cid,
            concat(
                upper(diagnostico_cid), '\n', upper(resumo_internacao)
            ) as motivo_atendimento,
        from {{ ref("raw_prontuario_mv__evolucao") }}
    ),

    -- Alta
    alta as (
        select
            id_atendimento,
            atendimento_datahora,
            alta_medica_datahora,
            profissional_nome,
            cid_principal,
            procedimentos_realizados,
            evolucao_paciente,
            plano_alta_orientacao_enfermagem,
            orientacao_medica,
        from {{ ref("raw_prontuario_mv__alta") }}
    ),

    -- Condições (CID)
    condicoes as (
        select 
            id_atendimento,
            cast(atendimento_datahora as date) as data_diagnostico,
            id_cid,
            descricao
        from atendimento a
        join {{ ref("dim_condicao_cid10") }} c on c.id = a.id_cid
        union all
        select
            id_atendimento,
            cast(data_diagnostico as date) as data_diagnostico,
            id_cid,
            descricao
        from parecer p
        join {{ ref("dim_condicao_cid10") }} c on c.id = p.id_cid
        where id_cid is not null
        union all
        select
            id_atendimento,
            cast(a.atendimento_datahora as date) as data_diagnostico,
            cid_principal as id_cid,
            descricao
        from alta a
        join {{ ref("dim_condicao_cid10") }} c on c.id = a.cid_principal
        where cid_principal is not null
    ),

    profissional as (
        select p.nome, p.cns, p.cpf, p.id_cbo, cbo.descricao
        from {{ ref("raw_prontuario_mv__profissional") }} p
        left join {{ ref("raw_datasus__cbo") }} cbo using (id_cbo)
    ),

    profissional_saude_responsavel as (
        select id_atendimento, profissional_nome,
        from alta
        union all
        select id_atendimento, profissional_nome,
        from anamnese
        union all
        select id_atendimento, profissional_nome,
        from bam
        union all
        select id_atendimento, profissional_nome,
        from evolucao
    ),

    profissional_saude_enriquecido as (
        select distinct
            psr.id_atendimento,
            psr.profissional_nome,
            p.cns,
            p.cpf,
            p.id_cbo,
            p.descricao
        from profissional_saude_responsavel psr
        left join profissional p on upper(psr.profissional_nome) = upper(p.nome)
    -- Até o momento o nome do profissional é a única informação que temos para
    -- realizar o match, o que pode gerar alguns erros de associação.
    ),

    condicoes_agg as (
        select
            id_atendimento,
            array_agg(
                struct(
                    id_cid,
                    descricao,
                    cast(null as string) as situacao,
                    cast(data_diagnostico as string) as data_diagnostico
                )
            ) as condicoes_agregadas
        from condicoes
        group by id_atendimento
    ),

    final as (
        select
            id_hci,
            paciente_cpf as cpf,

            case
                when atendimento_tipo like 'AMBULATORIAL'
                then 'Agendada'
                when atendimento_tipo like 'INTERNAÇÃO'
                then 'Internação'
                when atendimento_tipo like 'URGÊNCIA'
                then 'Demanda Espontânea'
                else upper(atendimento_tipo)
            end as tipo,
            initcap(atendimento_tipo) as subtipo,

            {{ parse_and_filter_future_datetime("atendimento.atendimento_datahora") }}
            as entrada_datahora,
            {{ parse_and_filter_future_datetime("alta_datahora") }} as saida_datahora,

            -- Exames Realizados 
            -- Procedimentos Realizados
            upper(procedimentos_realizados) as procedimentos_realizados,

            -- Motivo do Atendimento
            coalesce(
                bam.motivo_atendimento, an.motivo_atendimento, ev.motivo_atendimento
            ) as motivo_atendimento,

            -- Desfecho do Atendimento
            coalesce(
                upper(atendimento_desfecho),
                upper(bam.conduta_proposta),
                upper(bam.destino_paciente),
                upper(ev.conduta_adotada),
                upper(ev.planejamento_terapeutico),
                upper(an.conduta_proposta),
                upper(an.destino_paciente)
            ) as desfecho_atendimento,

            -- Condicoes
            c.condicoes_agregadas as condicoes,

            -- Medidas
            struct(
                an.altura as altura,
                an.superficie_corporal as circunferencia_abdominal,
                coalesce(
                    bam.frequencia_cardiaca, an.frequencia_cardiaca
                ) as frequencia_cardiaca,
                coalesce(
                    bam.frequencia_respiratoria, an.frequencia_respiratoria
                ) as frequencia_respiratoria,
                cast(null as float64) as glicemia,
                cast(null as float64) as hemoglobina_glicada,
                cast(null as float64) as imc,
                cast(null as float64) as peso,
                coalesce(
                    bam.pressao_arterial_sistolica, an.pressao_arterial_sistolica
                ) as pressao_sistolica,
                coalesce(
                    bam.pressao_arterial_diastolica, an.pressao_arterial_diastolica
                ) as pressao_diastolica,
                cast(null as string) as pulso_ritmo,
                coalesce(
                    bam.saturacao_oxigenio, an.saturacao_oxigenio
                ) as saturacao_oxigenio,
                coalesce(bam.temperatura, an.temperatura) as temperatura
            ) as medidas,

            -- Obito Indicador 
            -- Obitos são indicados no desfecho do atendimento com os seguintes
            -- desfechos
            -- - "OBITO COM DECLARACAO DE OBITO FORNECIDA PELO INSTITUTO MEDIC"
            -- - "OBITO COM DECLARACAO DE OBITO FORNECIDA PELO MEDICO ASSISTEN"
            if(atendimento_desfecho like '%OBITO%', true, false) as obito_indicador,

            -- Prescricoes 
            -- Campo aberto, exige mineração de texto para se 
            -- encaixar no formato atual do episódio assitencial
            -- Medicamentos Administrados
            -- Campo aberto, exige mineração de texto para se 
            -- encaixar no formato atual do episódio assitencial
            -- Estabelecimento
            struct(
                id_cnes,
                {{ proper_estabelecimento("e.nome_acentuado") }} as nome,
                e.tipo_sms as estabelecimento_tipo
            ) as estabelecimento,

            -- Profissional
            struct(
                cast(null as string) as id,
                p.cpf as cpf,
                p.cns as cns,
                p.profissional_nome as nome,
                p.descricao as especialidade
            ) as profissional_saude_responsavel,

            -- Prontuario
            struct(
                concat(id_cnes, '.', id_atendimento) as id_prontuario_global,
                id_atendimento as id_prontuario_local,
                'mv' as fornecedor
            ) as prontuario,

            -- Metadados
            struct(
                updated_at,
                loaded_at as imported_at,
                current_datetime('America/Sao_Paulo') as processed_at
            ) as metadados,
            date(atendimento.atendimento_datahora) as data_particao,
            safe_cast(cpf as int64) as cpf_particao

        from atendimento
        left join bam using (id_atendimento)
        left join alta a using (id_atendimento)
        left join evolucao ev using (id_atendimento)
        left join anamnese an using (id_atendimento)
        left join condicoes_agg c using (id_atendimento)
        left join profissional_saude_enriquecido p using (id_atendimento)
        left join {{ ref("dim_estabelecimento") }} e using (id_cnes)
    )

select *
from final
qualify row_number() over (partition by id_hci order by metadados.updated_at desc) = 1
