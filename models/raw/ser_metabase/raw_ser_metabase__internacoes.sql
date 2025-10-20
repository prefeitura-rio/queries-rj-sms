-- noqa: disable=LT08
{{
  config(
    enabled=true,
    schema="ser_matabase",
    alias="internacoes",
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id_solicitacao',
    partition_by={
      "field": "data_solicitacao",
      "data_type": "date",
      "granularity": "month",
    },
    cluster_by=['id_cnes_unidade_origem', 'id_cnes_unidade_executante', 'procedimento'],
    on_schema_change='sync_all_columns'
  )
}}


with
{% if is_incremental() %}
    part as (
        select
            date(max(data_solicitacao)) as data_solicitacao_ultima
        from {{ this }}
    ),
{% endif %}

source as (
    select
        solicitacao_id,
        estadoAnterior,
        estadoAtual,
        data_evento,
        tipo_de_leito,
        tipointernacao,
        nAcaoJudicial,
        juiz,
        pena,
        decisaoJuiz,
        prazo,
        reu,
        dt_reserva,
        data_solicitacao,
        tipo_leito_regulado,
        observacao,
        dt_inicio_internacao,
        dt_termino_internacao,
        estadoSolicitacao,
        motivo_alta,
        operacao_internacao_id,
        paciente_nome,
        paciente_dataNacimento,
        municipio_paciente_codigo_ibge,
        cnes_unidade_origem,
        cnes_unidade_executante,
        motivo_cancelamento_solicitacao,
        especialidade,
        codigo_cid,
        procedimento,
        cns,
        data_alta,
        carater_internacao,
        Sala_Vermelha,
        Infarto_agudo,
        Infarto_agudo_supraST,
        Infarto_agudo_trombolizado,
        data_extracao
    from {{ source('brutos_ser_metabase_staging', 'FATO_INTERNACAO') }}
    {% if is_incremental() %}
        where 1=1
            and date(data_particao) >= (select data_solicitacao_ultima from part)
    {% endif %}
),

transform as (
    select
        -- pk
        safe_cast(split(solicitacao_id, '.')[0] as int) as id_solicitacao,

        -- datas
        date(safe_cast(
            {{ process_null("data_solicitacao") }} as timestamp), 'America/Sao_Paulo'
        ) as data_solicitacao,

        date(safe_cast(
            {{ process_null("dt_reserva") }} as timestamp), 'America/Sao_Paulo'
        ) as data_reserva,

        date(safe_cast(
            {{ process_null("dt_inicio_internacao") }} as timestamp), 'America/Sao_Paulo'
        ) as data_internacao_inicio,

        date(safe_cast(
            {{ process_null("dt_termino_internacao") }} as timestamp), 'America/Sao_Paulo'
        ) as data_internacao_termino,

        date({{process_null("data_alta")}}) as data_alta, -- diferenca da data_internacao_termino?

        date(safe_cast(
            {{ process_null("data_evento") }} as timestamp), 'America/Sao_Paulo'
        ) as data_atualizacao_registro,

        -- unidades envolvidas 
        lpad(cast(safe_cast(split({{ process_null("cnes_unidade_origem") }}, '.')[0] as int) as string), 7, '0') as id_cnes_unidade_origem,
        lpad(cast(safe_cast(split({{ process_null("cnes_unidade_executante") }}, '.')[0] as int) as string), 7, '0') as id_cnes_unidade_executante,

        -- qualificacao do procedimento
        carater_internacao as internacao_carater,
        upper(trim({{ process_null("tipo_de_leito") }})) as leito_tipo,
        upper(trim({{ process_null("tipo_leito_regulado") }})) as leito_regulado_tipo,
        upper(trim({{process_null("especialidade")}})) as especialidade,
        upper(trim({{process_null("procedimento")}})) as procedimento,
        {{ process_null("codigo_cid") }} as cid,
        tipointernacao as internacao_tipo,

        -- estado da solicitacao
        estadoSolicitacao as solicitacao_estado,
        estadoAtual as solicitacao_estado_atual, -- qual a diferenca para estadoSolicitacao?
        estadoAnterior as solicitacao_estado_anterior,
        {{ process_null("motivo_alta") }} as justificatica_alta,
        motivo_cancelamento_solicitacao as justificativa_cancelamento,

        -- paciente
        lpad(cast(safe_cast(split(cns, '.')[0] as int) as string), 15, '0') as paciente_cns,
        {{clean_name_string(process_null("paciente_nome"))}} as paciente_nome,
        safe_cast({{ process_null("paciente_dataNacimento") }} as date) as paciente_data_nascimento,
        safe_cast(split(municipio_paciente_codigo_ibge, '.')[0] as int) as id_paciente_municipio_ibge,

        -- indicadores
        upper(trim({{ process_null("Sala_Vermelha") }})) as indicador_sala_vermelha,
        upper(trim({{ process_null("Infarto_agudo") }})) as indicador_infarto_agudo,
        upper(trim({{ process_null("Infarto_agudo_supraST") }})) as indicador_infarto_agudo_supra_st,
        upper(trim({{ process_null("Infarto_agudo_trombolizado") }})) as indicador_infarto_agudo_trombolizado,

        -- judicializacao
        {{ process_null(clean_name_string("juiz")) }} as jud_juiz_nome,
        nAcaoJudicial as jud_acao_judicial_id,
        pena as jud_pena,
        decisaoJuiz as jud_decisao_juiz,
        prazo as jud_prazo,
        reu as jud_reu,

        -- ???
        operacao_internacao_id as id_operacao_internacao, -- o que é isso?
        observacao, -- o que é isso?

        -- metadado
        safe_cast({{ process_null("data_extracao")}} as timestamp) as data_extracao
    from source
)

select
    *,
    row_number() over (partition by id_solicitacao order by data_atualizacao_registro desc) as row_num
from transform
qualify row_num = 1
