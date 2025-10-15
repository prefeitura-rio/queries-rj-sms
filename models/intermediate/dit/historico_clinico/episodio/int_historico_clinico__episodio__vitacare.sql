{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_vitacare",
        materialized="incremental",
        incremental_strategy='merge',
        unique_key=['id_hci'],
        cluster_by=['id_hci'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}


with
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- Separação de Atendimentos
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    bruto_atendimento as (
        select * from {{ ref("raw_prontuario_vitacare__atendimento") }}
        {% if is_incremental() %} where data_particao >= {{ partitions_to_replace }} {% endif %}
    -- where data_particao = "2024-08-01"
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Profissional
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    dim_profissional as (
        select
            ep.cns as pk, ep.id_profissional_sus as id, ep.cns, ep.nome, ep.cpf, c.cbo
        from {{ ref("dim_profissional_saude") }} as ep, unnest(ep.cbo) as c
        qualify row_number() over (partition by cpf order by id desc) = 1
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Estabelecimento
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    dim_estabelecimento as (
        select
            id_cnes as pk,
            struct(
                id_cnes,
                {{ proper_estabelecimento("nome_acentuado") }} as nome,
                tipo_sms as estabelecimento_tipo
            ) as estabelecimento
        from {{ ref("dim_estabelecimento") }}
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Condições
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    cid_descricao as (
        select distinct regexp_replace(id,r'\.','') as id, descricao
        from {{ ref("dim_condicao_cid10") }}
        union all
        select distinct regexp_replace(categoria.id,r'\.','') as id, categoria.descricao as descricao
        from {{ ref("dim_condicao_cid10") }}
    ),
    condicoes as (
        select distinct
            id_prontuario_global as fk_atendimento,

            regexp_replace(
                json_extract_scalar(condicao_json, "$.cod_cid10"),
                r'\.',
                ''
             ) as id,

            case
                when json_extract_scalar(condicao_json, "$.estado") = "N.E"
                then "NAO ESPECIFICADO"
                else json_extract_scalar(condicao_json, "$.estado")
            end as situacao,

            json_extract_scalar(
                condicao_json, "$.data_diagnostico"
            ) as data_diagnostico,

        from bruto_atendimento, unnest(json_extract_array(condicoes)) as condicao_json
    ),

    dim_condicoes_atribuidas as (
        select
            fk_atendimento,
            array_agg(
                struct(
                    condicoes.id as id,
                    cid_descricao.descricao,
                    condicoes.situacao as situacao,
                    condicoes.data_diagnostico as data_diagnostico
                )
                order by data_diagnostico desc
            ) as condicoes
        from condicoes
        left join
            (select distinct id, descricao from cid_descricao) as cid_descricao
            on condicoes.id = cid_descricao.id
        group by fk_atendimento
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Procedimentos
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    procedimentos as (
        select
            id_prontuario_global as fk_atendimento,
            case
                when
                    json_extract_scalar(procedimentos_json, '$.procedimento_clinico')
                    = ''
                then null
                else
                    upper(
                        json_extract_scalar(
                            procedimentos_json, '$.procedimento_clinico'
                        )
                    )
            end as procedimento,
            case
                when json_extract_scalar(procedimentos_json, '$.observacao') = '' or json_extract_scalar(procedimentos_json, '$.observacao') is null 
                then ''
                else upper(json_extract_scalar(procedimentos_json, '$.observacao'))
            end as observacao,

        from
            bruto_atendimento,
            unnest(
                json_extract_array(soap_plano_procedimentos_clinicos)
            ) as procedimentos_json
    ),
    procedimentos_sem_nulos as (
        select
            fk_atendimento,
            concat(procedimento, '\n', observacao) as procedimentos_realizados
        from procedimentos
        where
            procedimentos.procedimento is not null
            or procedimentos.observacao is not null
    ),
    dim_procedimentos_realizados as (
        select
            fk_atendimento,
            string_agg(procedimentos_realizados, '\n\n') as procedimentos_realizados
        from procedimentos_sem_nulos
        group by 1
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Medicamento Prescrito
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    materiais as (
        select id_material, descricao, concentracao from {{ ref("dim_material") }}
    ),
    prescricoes as (
        select
            id_prontuario_global as fk_atendimento,
            replace(
                json_extract_scalar(prescricoes_json, "$.cod_medicamento"), "-", ""
            ) as id,
            upper(json_extract_scalar(prescricoes_json, "$.nome_medicamento")) as nome,
            json_extract_scalar(prescricoes_json, "$.uso_continuado") as uso_continuo
        from
            bruto_atendimento,
            unnest(json_extract_array(prescricoes)) as prescricoes_json
    ),
    dim_prescricoes_atribuidas as (
        select
            fk_atendimento,
            array_agg(
                struct(
                    prescricoes.id,
                    coalesce(materiais.descricao, prescricoes.nome) as nome,
                    materiais.concentracao,
                    prescricoes.uso_continuo
                )
            ) as prescricoes
        from prescricoes
        left join materiais on prescricoes.id = materiais.id_material
        group by fk_atendimento
    ),

    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Medidas
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    medidas as (
        select
            *,
            case
                when json_extract_scalar(indicadores_json, '$.nome') = ''
                then null
                else
                    lower(
                        {{
                            remove_accents_upper(
                                "json_extract_scalar(indicadores_json, '$.nome')"
                            )
                        }}
                    )
            end as nome,
            case
                when json_extract_scalar(indicadores_json, '$.valor') = ''
                then null
                else json_extract_scalar(indicadores_json, '$.valor')
            end as valor
        from
            bruto_atendimento,
            unnest(json_extract_array(indicadores)) as indicadores_json
        qualify row_number() over (partition by id_hci,nome order by loaded_at asc) = 1
    ),

    medidas_padronizadas as (
        select
            id_prontuario_global as fk_atendimento,
            case
                when
                    nome in (
                        "circunferencia abdominal",
                        "frequencia cardiaca",
                        "frequencia respiratoria",
                        "pulso ritmo"
                    )
                then replace(nome, " ", "_")
                when nome = "glicemia (jejum)"
                then "glicemia"
                when nome = "hemoglobina glicada a1c (hba1c)"
                then "hemoglobina_glicada"
                when nome = "pressao arterial sistolica"
                then "pressao_sistolica"
                when nome = "pressao arterial diastolica"
                then "pressao_diastolica"
                when nome = "saturacao de o2"
                then "saturacao_oxigenio"
                else nome
            end as nome,
            lower({{ remove_accents_upper("valor") }}) as valor,
        from medidas
    ),

    medidas_numericas as (
        select * except (valor), safe_cast(valor as float64) as valor
        from medidas_padronizadas
        where nome <> "pulso_ritmo"
    ),

    medidas_numericas_pivot as (
        select
            fk_atendimento,
            {{
                dbt_utils.pivot(
                    "nome",
                    (
                        "altura",
                        "circunferencia_abdominal",
                        "frequencia_cardiaca",
                        "frequencia_respiratoria",
                        "glicemia",
                        "hemoglobina_glicada",
                        "imc",
                        "peso",
                        "pressao_sistolica",
                        "pressao_diastolica",
                        "saturacao_oxigenio",
                        "temperatura",
                    ),
                    agg="sum",
                    then_value="valor",
                    else_value="null",
                )
            }}
        from medidas_numericas
        group by fk_atendimento
    ),

    medida_categoricas as (
        select * from medidas_padronizadas where nome = "pulso_ritmo"
    ),

    medidas_categoricas_pivot as (
        select fk_atendimento, valor as pulso_ritmo from medida_categoricas
    ),

    medidas_unificadas as (
        select 
            fk_atendimento,
            if((altura > 20) and (altura < 230), altura, null) as altura,
            if((circunferencia_abdominal < 300) and (circunferencia_abdominal > 20), circunferencia_abdominal, null) as circunferencia_abdominal,
            if((frequencia_cardiaca > 30) and (frequencia_cardiaca < 220), frequencia_cardiaca, null) as frequencia_cardiaca,
            if((frequencia_respiratoria < 60) and (frequencia_respiratoria > 5), frequencia_respiratoria, null) as frequencia_respiratoria,
            if((glicemia < 300) and (glicemia > 40), glicemia, null) as glicemia, 
            if((hemoglobina_glicada < 30) and (hemoglobina_glicada > 0), hemoglobina_glicada, null) as hemoglobina_glicada, 
            if((imc < 300) and (imc > 0), imc, null) as imc, 
            if((peso < 500) and (peso > 0), peso, null) as peso, 
            if((pressao_sistolica < 240) and (pressao_sistolica > 7), pressao_sistolica, null) as pressao_sistolica, 
            if((pressao_diastolica < 150) and (pressao_diastolica > 5), pressao_diastolica, null) as pressao_diastolica, 
            pulso_ritmo, 
            if((saturacao_oxigenio < 101) or (saturacao_oxigenio > 50), saturacao_oxigenio, null) as saturacao_oxigenio, 
            if((temperatura < 41) or (temperatura > 33), temperatura, null) as temperatura, 

        from medidas_numericas_pivot
        full outer join medidas_categoricas_pivot using (fk_atendimento)
    ),

    dim_medidas as (

        select
            fk_atendimento,
            struct(
                altura,
                circunferencia_abdominal,
                frequencia_cardiaca,
                frequencia_respiratoria,
                glicemia,
                hemoglobina_glicada,
                imc,
                peso,
                pressao_sistolica,
                pressao_diastolica,
                pulso_ritmo,
                saturacao_oxigenio,
                temperatura
            ) as medidas
        from medidas_unificadas
    ),

    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- FATO: Atendimento
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    fato_atendimento as (
        select
            atendimento.id_hci,

            -- Paciente
            {{ process_null("atendimento.cpf") }} as cpf,

            -- Tipo e Subtipo
            safe_cast(
                case
                    when eh_coleta = 'True'
                    then 'Exames Complementares'
                    when vacinas != '[]'
                    then 'Vacinação'
                    when datahora_marcacao is null
                    then 'Demanda Espontânea'
                    else 'Agendada'
                end as string
            ) as tipo,
            safe_cast(
                case
                    when eh_coleta = 'True'
                    then 'N/A'
                    when vacinas != '[]'
                    then
                        json_extract_scalar(
                            json_extract(vacinas, '$[0]'), '$.nome_vacina'
                        )
                    else nullif(tipo, '')
                end as string
            ) as subtipo,

            -- Entrada e Saída
            safe_cast(
                if(datahora_inicio > current_date(),
                    null,
                    datahora_inicio) 
                as date) as entrada_datahora,
            safe_cast(
                if(datahora_fim > current_date(),
                null,
                datahora_fim)
                as date) as saida_datahora,

            -- Motivo e Desfecho
            upper(trim(soap_subjetivo_motivo)) as motivo_atendimento,
            upper(trim(soap_plano_observacoes)) as desfecho_atendimento,

            -- Condições
            dim_condicoes_atribuidas.condicoes,

            -- Medidas
            dim_medidas.medidas,

            -- Procedimentos
            trim(
                dim_procedimentos_realizados.procedimentos_realizados
            ) as procedimentos_realizados,

            -- Prescricoes
            dim_prescricoes_atribuidas.prescricoes,

            -- Estabelecimento
            dim_estabelecimento.estabelecimento,

            -- Profissional
            (
                select as struct
                    dim_profissional.id as id,
                    atendimento.cpf_profissional as cpf,
                    atendimento.cns_profissional as cns,
                    {{ proper_br("atendimento.nome_profissional") }} as nome,
                    safe_cast(cbo_descricao_profissional as string) as especialidade
            ) as profissional_saude_responsavel,

            -- Prontuário
            -- - `id_prontuario_global` é, em teoria, <CNES>.<ID local>
            -- - Extraímos o ID local com split('.')[1]
            struct(
                atendimento.id_prontuario_global,
                safe_cast(
                    split(atendimento.id_prontuario_global, ".")[safe_offset(1)]
                    as string
                ) as id_prontuario_local,
                'vitacare' as fornecedor
            ) as prontuario,

            -- Metadados
            struct(
                updated_at, loaded_at as imported_at, datetime(current_timestamp(),'America/Sao_Paulo') as processed_at
            ) as metadados,

            atendimento.data_particao,
            safe_cast({{ process_null("atendimento.cpf") }} as int64) as cpf_particao,

        from bruto_atendimento as atendimento
        left join
            dim_estabelecimento on atendimento.cnes_unidade = dim_estabelecimento.pk
        left join dim_profissional on atendimento.cns_profissional = dim_profissional.pk
        left join
            dim_condicoes_atribuidas
            on atendimento.id_prontuario_global = dim_condicoes_atribuidas.fk_atendimento
        left join dim_medidas on atendimento.id_prontuario_global = dim_medidas.fk_atendimento
        left join
            dim_procedimentos_realizados
            on atendimento.id_prontuario_global = dim_procedimentos_realizados.fk_atendimento
        left join
            dim_prescricoes_atribuidas
            on atendimento.id_prontuario_global = dim_prescricoes_atribuidas.fk_atendimento
    ),

    episodios_validos as (select * from fato_atendimento where id_hci is not null)

-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- Finalização
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from episodios_validos

