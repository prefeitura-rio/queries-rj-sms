{{
    config(
        alias="admissao_neo_natal",
        materialized="incremental",
        schema="brutos_prontuario_mv",
        unique_key="id_hci",
        tags=["mv"],
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


with

    source as (
        select *
        from {{ source("brutos_prontuario_mv_api_staging", "admissao_continuo") }}
        {% if is_incremental() %}
            where
                timestamp_trunc(datalake_loaded_at, day) >= timestamp(
                    date_sub(current_date('America/Sao_Paulo'), interval 7 day)
                )
        {% endif %}
    ),

    admissao as (
        select
            payload_cnes,
            json_extract_scalar(
                `data`, '$.data_hora_fechamento'
            ) as data_hora_fechamento,
            json_extract_scalar(`data`, '$.paciente_nome_rn') as paciente_nome_rn,
            json_extract_scalar(`data`, '$.idade_mae') as idade_mae,
            json_extract_scalar(`data`, '$.gestacoes_numero') as gestacoes_numero,
            json_extract_scalar(`data`, '$.partos_numero') as partos_numero,
            json_extract_scalar(`data`, '$.aborto_historico') as aborto_historico,
            json_extract_scalar(`data`, '$.pre_natal_realizado') as pre_natal_realizado,
            json_extract_scalar(
                `data`, '$.pre_natal_qtd_consultas'
            ) as pre_natal_qtd_consultas,
            json_extract_scalar(
                `data`, '$.data_ultima_menstruacao'
            ) as data_ultima_menstruacao,
            json_extract_scalar(`data`, '$.data_provavel_parto') as data_provavel_parto,
            json_extract_scalar(
                `data`, '$.idade_gestacional_semanas'
            ) as idade_gestacional_semanas,
            json_extract_scalar(
                `data`, '$.ultrassonografia_detalhes'
            ) as ultrassonografia_detalhes,
            json_extract_scalar(`data`, '$.grupo_sanguineo_mae') as grupo_sanguineo_mae,
            json_extract_scalar(
                `data`, '$.exame_coombs_indireto'
            ) as exame_coombs_indireto,
            json_extract_scalar(`data`, '$.exame_sifilis') as exame_sifilis,
            json_extract_scalar(`data`, '$.exame_hiv') as exame_hiv,
            json_extract_scalar(`data`, '$.outros_exames') as outros_exames,
            json_extract_scalar(
                `data`, '$.intercorrencias_gestacao'
            ) as intercorrencias_gestacao,
            json_extract_scalar(`data`, '$.uso_drogas_gestacao') as uso_drogas_gestacao,
            json_extract_scalar(
                `data`, '$.data_hora_nascimento'
            ) as data_hora_nascimento,
            json_extract_scalar(`data`, '$.sofrimento_fetal') as sofrimento_fetal,
            json_extract_scalar(`data`, '$.tipo_parto') as tipo_parto,
            json_extract_scalar(
                `data`, '$.apresentacao_rn_parto'
            ) as apresentacao_rn_parto,
            json_extract_scalar(`data`, '$.uso_vacuo_extrator') as uso_vacuo_extrator,
            json_extract_scalar(`data`, '$.tipo_anestesia') as tipo_anestesia,
            json_extract_scalar(
                `data`, '$.drogas_utilizadas_parto'
            ) as drogas_utilizadas_parto,
            json_extract_scalar(`data`, '$.observacoes_parto') as observacoes_parto,
            json_extract_scalar(`data`, '$.tempo_bolsa_rota') as tempo_bolsa_rota,
            json_extract_scalar(
                `data`, '$.liquido_amniotico_aspecto_volume'
            ) as liquido_amniotico_aspecto_volume,
            json_extract_scalar(`data`, '$.avaliacao_apgar') as avaliacao_apgar,
            json_extract_scalar(
                `data`, '$.manobras_inicias_parto'
            ) as manobras_inicias_parto,
            json_extract_scalar(`data`, '$.manobras_reanimacao') as manobras_reanimacao,
            json_extract_scalar(
                `data`, '$.descricao_reanimacao'
            ) as descricao_reanimacao,
            json_extract_scalar(
                `data`, '$.contato_seio_materno'
            ) as contato_seio_materno,
            json_extract_scalar(`data`, '$.contato_pele_pele') as contato_pele_pele,
            json_extract_scalar(`data`, '$.banho_rn') as banho_rn,
            json_extract_scalar(`data`, '$.profilaxia_inicial') as profilaxia_inicial,
            json_extract_scalar(`data`, '$.exame_fisico_rn') as exame_fisico_rn,
            json_extract_scalar(
                `data`, '$.diagnostico_primario'
            ) as diagnostico_primario,
            json_extract_scalar(
                `data`, '$.diagnostico_secundario'
            ) as diagnostico_secundario,
            json_extract_scalar(
                `data`, '$.encaminhamento_pos_nacimento'
            ) as encaminhamento_pos_nacimento,
            json_extract_scalar(
                `data`, '$.plano_terapeutico_rn'
            ) as plano_terapeutico_rn,
            json_extract_scalar(`data`, '$.metas_tratamento') as metas_tratamento,
            json_extract_scalar(
                `data`, '$.profissional_saude_nome'
            ) as profissional_saude_nome,
            json_extract_scalar(
                `data`, '$.unidade_atendimento_nome'
            ) as unidade_atendimento_nome,
            json_extract_scalar(`data`, '$.nome_paciente') as nome_paciente,
            json_extract_scalar(`data`, '$.cpf_paciente') as cpf_paciente,
            json_extract_scalar(`data`, '$.cns_paciente') as cns_paciente,
            json_extract_scalar(
                `data`, '$.nome_social_paciente'
            ) as nome_social_paciente,
            json_extract_scalar(`data`, '$.numero_atendimento') as numero_atendimento,  -- ID
            json_extract_scalar(
                `data`, '$.data_nascimento_paciente'
            ) as data_nascimento_paciente,
            json_extract_scalar(`data`, '$.data_atendimento') as data_atendimento,
            json_extract_scalar(`data`, '$.sexo_paciente') as sexo_paciente,
            json_extract_scalar(`data`, '$.pcd') as pcd,
            json_extract_scalar(`data`, '$.nome_mae') as nome_mae,
            json_extract_scalar(`data`, '$.exame_vdrl') as exame_vdrl,
            json_extract_scalar(`data`, '$.titulacaoVDRL') as titulacaovdrl,
            json_extract_scalar(`data`, '$.titulacao_hiv') as titulacao_hiv,
            json_extract_scalar(`data`, '$.data_vdrl') as data_vdrl,
            json_extract_scalar(`data`, '$.data_hiv') as data_hiv,
            json_extract_scalar(`data`, '$.usg') as usg,
            json_extract_scalar(`data`, '$.data_usg') as data_usg,
            json_extract_scalar(`data`, '$.outras_sorologias') as outras_sorologias,
            json_extract_scalar(`data`, '$.volume') as volume,
            json_extract_scalar(
                `data`, '$.batimento_cardiaco_1min'
            ) as batimento_cardiaco_1min,
            json_extract_scalar(`data`, '$.respiracao_1min') as respiracao_1min,
            json_extract_scalar(`data`, '$.tonus_1min') as tonus_1min,
            json_extract_scalar(`data`, '$.irritabilidade_1min') as irritabilidade_1min,
            json_extract_scalar(`data`, '$.cor_1min') as cor_1min,
            json_extract_scalar(`data`, '$.total_apgar_1min') as total_apgar_1min,
            json_extract_scalar(
                `data`, '$.batimento_cardiaco_5min'
            ) as batimento_cardiaco_5min,
            json_extract_scalar(`data`, '$.respiracao_5min') as respiracao_5min,
            json_extract_scalar(`data`, '$.tonus_5min') as tonus_5min,
            json_extract_scalar(`data`, '$.irritabilidade_5min') as irritabilidade_5min,
            json_extract_scalar(`data`, '$.cor_5min') as cor_5min,
            json_extract_scalar(`data`, '$.total_apgar_5min') as total_apgar_5min,
            json_extract_scalar(
                `data`, '$.batimento_cardiaco_10min'
            ) as batimento_cardiaco_10min,
            json_extract_scalar(`data`, '$.respiracao_10min') as respiracao_10min,
            json_extract_scalar(`data`, '$.tonus_10min') as tonus_10min,
            json_extract_scalar(
                `data`, '$.irritabilidade_10min'
            ) as irritabilidade_10min,
            json_extract_scalar(`data`, '$.cor_10min') as cor_10min,
            json_extract_scalar(`data`, '$.total_apgar_10min') as total_apgar_10min,
            json_extract_scalar(`data`, '$.sn_secagem') as sn_secagem,
            json_extract_scalar(`data`, '$.sn_aspiracao_oro') as sn_aspiracao_oro,
            json_extract_scalar(
                `data`, '$.sn_aspiracao_gastrica'
            ) as sn_aspiracao_gastrica,
            json_extract_scalar(
                `data`, '$.sn_aspiracao_traqueal'
            ) as sn_aspiracao_traqueal,
            json_extract_scalar(`data`, '$.sn_outras') as sn_outras,
            json_extract_scalar(`data`, '$.ds_outras') as ds_outras,
            json_extract_scalar(`data`, '$.sn_mascara_ambu') as sn_mascara_ambu,
            json_extract_scalar(`data`, '$.sn_intubacao') as sn_intubacao,
            json_extract_scalar(`data`, '$.sn_massagem_card') as sn_massagem_card,
            json_extract_scalar(`data`, '$.sn_drogas') as sn_drogas,
            json_extract_scalar(`data`, '$.ds_drogas') as ds_drogas,
            json_extract_scalar(
                `data`, '$.ds_justificativa_contato_seio'
            ) as ds_justificativa_contato_seio,
            json_extract_scalar(
                `data`, '$.ds_justificativa_contato_pele'
            ) as ds_justificativa_contato_pele,
            json_extract_scalar(
                `data`, '$.ds_justificativa_banho'
            ) as ds_justificativa_banho,
            json_extract_scalar(`data`, '$.sn_crede') as sn_crede,
            json_extract_scalar(`data`, '$.sn_vit_k') as sn_vit_k,
            json_extract_scalar(`data`, '$.sn_vacina_hep_b') as sn_vacina_hep_b,
            json_extract_scalar(`data`, '$.tipoAtendimento') as tipoatendimento,
            json_extract_scalar(
                `data`, '$.especialidadeAtendimento'
            ) as especialidadeatendimento,
            json_extract_scalar(`data`, '$.tp_sexo_rn') as tp_sexo_rn,
            json_extract_scalar(`data`, '$.vl_peso_rn') as vl_peso_rn,
            json_extract_scalar(
                `data`, '$.vl_perimetro_cefalico_rn'
            ) as vl_perimetro_cefalico_rn,
            json_extract_scalar(
                `data`, '$.vl_perimetro_toraxico_rn'
            ) as vl_perimetro_toraxico_rn,
            json_extract_scalar(
                `data`, '$.vl_perimetro_abdominal_rn'
            ) as vl_perimetro_abdominal_rn,
            json_extract_scalar(`data`, '$.hr_nascimento_rn') as hr_nascimento_rn,
            json_extract_scalar(`data`, '$.vl_altura_rn') as vl_altura_rn,
            datalake_loaded_at,
            source_updated_at,
        from source

    ),  -- TODO: Aplicar os casts de colunas
    admissao_renomeado as (
        select
            {{ process_null("numero_atendimento") }} as id_atendimento,

            -- Estabelecimento de Saúde
            {{ process_null("payload_cnes") }} as id_cnes,
            {{ process_null("unidade_atendimento_nome") }} as estabelecimento_nome,

            -- Atendimento
            safe.parse_datetime(
                '%Y/%m/%d %H:%M:%S', data_atendimento
            ) as atendimento_datahora,
            case
                when tipoatendimento like 'A'
                then 'AMBULATORIAL'
                when tipoatendimento like 'E'
                then 'EXTERNO'
                when tipoatendimento like 'I'
                then 'INTERNAÇÃO'
                when tipoatendimento like 'U'
                then 'URGÊNCIA'
                else tipoatendimento
            end as atendimento_tipo,
            {{ process_null("especialidadeAtendimento") }} as atendimento_especialidade,
            {{ process_null("profissional_saude_nome") }} as profissional_nome,
            {{ process_null("diagnostico_primario") }} as diagnostico_primario,
            {{ process_null("diagnostico_secundario") }} as diagnostico_secundario,
            safe.parse_datetime(
                '%Y/%m/%d %H:%M:%S', data_hora_fechamento
            ) as data_hora_fechamento,

            -- Paciente (Mãe/Gestante)
            {{ process_null("nome_paciente") }} as paciente_nome,
            {{ process_null("cpf_paciente") }} as paciente_cpf,
            {{ process_null("cns_paciente") }} as paciente_cns,
            {{ process_null("nome_social_paciente") }} as paciente_nome_social,
            safe.parse_datetime(
                '%Y/%m/%d %H:%M:%S', data_nascimento_paciente
            ) as paciente_data_nascimento,
            {{ process_null("sexo_paciente") }} as paciente_sexo,
            {{ process_null("pcd") }} as paciente_pcd,
            {{ process_null("nome_mae") }} as paciente_mae_nome,
            {{ process_null("idade_mae") }} as paciente_mae_idade,

            -- Gestação e Pré-natal
            safe_cast(
                {{ process_null("gestacoes_numero") }} as int64
            ) as gestacoes_numero,
            safe_cast({{ process_null("partos_numero") }} as int64) as partos_numero,
            safe_cast(
                {{ process_null("aborto_historico") }} as int64
            ) as aborto_historico,
            {{ process_null("pre_natal_realizado") }} as pre_natal_realizado,
            {{ process_null("pre_natal_qtd_consultas") }} as pre_natal_consultas,
            safe.parse_date(
                '%d/%m/%Y', data_ultima_menstruacao
            ) as data_ultima_menstruacao,
            safe.parse_date('%d/%m/%Y', data_provavel_parto) as parto_data_provavel,
            {{ process_null("idade_gestacional_semanas") }}
            as idade_gestacional_semanas,
            {{ process_null("intercorrencias_gestacao") }} as intercorrencias_gestacao,
            {{ process_null("uso_drogas_gestacao") }} as uso_drogas_gestacao,

            -- Exames
            {{ process_null("grupo_sanguineo_mae") }} as grupo_sanguineo_mae,
            {{ process_null("exame_coombs_indireto") }} as exame_coombs_indireto,
            {{ process_null("exame_sifilis") }} as exame_sifilis,
            {{ process_null("exame_hiv") }} as exame_hiv,
            {{ process_null("outros_exames") }} as exame_outros,
            {{ process_null("ultrassonografia_detalhes") }} as exame_ultrassonografia,
            {{ process_null("exame_vdrl") }} as exame_vdrl,
            {{ process_null("titulacaoVDRL") }} as exame_titulacao_vdrl,
            {{ process_null("titulacao_hiv") }} as exame_titulacao_hiv,
            safe.parse_date('%d/%m/%Y', data_vdrl) as exame_data_vdrl,
            safe.parse_date('%d/%m/%Y', data_hiv) as exame_data_hiv,
            {{ process_null("usg") }} as exame_usg,
            {{ process_null("data_usg") }} as exame_usg_data,
            {{ process_null("outras_sorologias") }} as outras_sorologias,
            {{ process_null("volume") }} as volume,

            -- Parto
            {{ process_null("data_hora_nascimento") }} as parto_nascimento_datahora,
            {{ process_null("tipo_parto") }} as parto_tipo,
            {{ process_null("tipo_anestesia") }} as parto_anestesia,
            {{ process_null("drogas_utilizadas_parto") }} as parto_drogas_utilizadas,
            {{ process_null("observacoes_parto") }} as parto_observacoes,
            {{ process_null("tempo_bolsa_rota") }} as parto_tempo_bolsa_rota,
            {{ process_null("liquido_amniotico_aspecto_volume") }}
            as parto_liquido_amniotico,
            {{ process_null("sofrimento_fetal") }} as parto_sofrimento_fetal,
            {{ process_null("apresentacao_rn_parto") }}
            as parto_apresentacao_recem_nascido,
            {{ process_null("uso_vacuo_extrator") }} as parto_uso_vacuo_extrator,

            -- Recém-nascido (RN)
            {{ process_null("paciente_nome_rn") }} as recem_nascido_nome,
            {{ process_null("tp_sexo_rn") }} as recem_nascido_sexo,
            {{ process_null("hr_nascimento_rn") }} as recem_nascido_parto_datahora,
            {{ process_null("vl_peso_rn") }} as recem_nascido_peso,
            {{ process_null("vl_altura_rn") }} as recem_nascido_altura,
            {{ process_null("vl_perimetro_cefalico_rn") }}
            as recem_nascido_perimetro_cefalico,
            {{ process_null("vl_perimetro_toraxico_rn") }}
            as recem_nascido_perimetro_toraxico,
            {{ process_null("vl_perimetro_abdominal_rn") }}
            as recem_nascido_perimetro_abdominal,

            -- APGAR e Reanimação
            {{ process_null("avaliacao_apgar") }} as avaliacao_apgar,
            safe_cast(batimento_cardiaco_1min as int64) as batimento_cardiaco_1min,
            safe_cast(batimento_cardiaco_5min as int64) as batimento_cardiaco_5min,
            safe_cast(batimento_cardiaco_10min as int64) as batimento_cardiaco_10min,
            safe_cast(respiracao_1min as int64) as respiracao_1min,
            safe_cast(respiracao_5min as int64) as respiracao_5min,
            safe_cast(respiracao_10min as int64) as respiracao_10min,
            safe_cast(cor_1min as int64) as cor_1min,
            safe_cast(cor_5min as int64) as cor_5min,
            safe_cast(cor_10min as int64) as cor_10min,
            safe_cast(tonus_1min as int64) as tonus_1min,
            safe_cast(tonus_5min as int64) as tonus_5min,
            safe_cast(tonus_10min as int64) as tonus_10min,
            safe_cast(irritabilidade_1min as int64) as irritabilidade_1min,
            safe_cast(irritabilidade_5min as int64) as irritabilidade_5min,
            safe_cast(irritabilidade_10min as int64) as irritabilidade_10min,
            {{ process_null("total_apgar_1min") }} as total_apgar_1min,
            {{ process_null("total_apgar_5min") }} as total_apgar_5min,
            {{ process_null("total_apgar_10min") }} as total_apgar_10min,
            {{ process_null("manobras_inicias_parto") }} as manobras_inicias_parto,
            {{ process_null("manobras_reanimacao") }} as manobras_reanimacao,
            {{ process_null("descricao_reanimacao") }} as descricao_reanimacao,
            {{ process_null("sn_secagem") }} as secagem_indicador,
            {{ process_null("sn_aspiracao_oro") }} as aspiracao_oro_indicador,
            {{ process_null("sn_aspiracao_gastrica") }} as aspiracao_gastrica_indicador,
            {{ process_null("sn_aspiracao_traqueal") }} as aspiracao_traqueal_indicador,
            {{ process_null("sn_outras") }} as outras_indicador,
            {{ process_null("ds_outras") }} as outras_descricao,
            {{ process_null("sn_mascara_ambu") }} as mascaras_ambu_indicador,
            {{ process_null("sn_intubacao") }} as intubacao,
            {{ process_null("sn_massagem_card") }} as massagem_cardiaca,
            {{ process_null("sn_drogas") }} as drogas_indicador,
            {{ process_null("ds_drogas") }} as drogas_descricao,

            -- Cuidados com o RN
            {{ process_null("contato_seio_materno") }} as contato_seio_materno,
            {{ process_null("ds_justificativa_contato_seio") }}
            as contato_seio_justificativa,
            {{ process_null("contato_pele_pele") }} as contato_pele_pele,
            {{ process_null("ds_justificativa_contato_pele") }}
            as contato_pele_justificativa,
            {{ process_null("banho_rn") }} as banho,
            {{ process_null("ds_justificativa_banho") }} as banho_justificativa,
            {{ process_null("profilaxia_inicial") }} as profilaxia_inicial,
            {{ process_null("sn_crede") }} as crede,
            {{ process_null("sn_vit_k") }} as vitamina_k,
            {{ process_null("sn_vacina_hep_b") }} as vacina_hepatite_b,
            {{ process_null("exame_fisico_rn") }} as exame_fisico_recem_nascido,
            {{ process_null("encaminhamento_pos_nacimento") }}
            as encaminhamento_pos_nacimento,
            {{ process_null("plano_terapeutico_rn") }} as plano_terapeutico,
            {{ process_null("metas_tratamento") }} as metas_tratamento,

            -- Metadados
            datetime(datalake_loaded_at, 'America/Sao_Paulo') as loaded_at,
            parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at) as updated_at,
            cast(datalake_loaded_at as date) as data_particao
        from admissao
        qualify
            row_number() over (
                partition by id_atendimento, id_cnes order by updated_at desc
            )
            = 1
    )

select
    {{ dbt_utils.generate_surrogate_key(["id_cnes", "id_atendimento"]) }} as id_hci, *
from admissao_renomeado
