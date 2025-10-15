
{% set last_partition = get_last_partition_date( this ) %}

with
source as (
    select
        solicitacao_id,
        estadoAnterior,
        estadoAtual,
        data_evento,
        tipo_de_leito,
        tipointernacao,
        nAcaoJudicial,
        nrOrdemJudicial,
        juiz,
        pena,
        decisaoJuiz,
        prazo,
        reu,
        dt_reserva,
        data_solicitacao,
        tipo_leito_regulado,
        observacao,
        evento,
        dt_inicio_internacao,
        dt_termino_internacao,
        estadoSolicitacao,
        motivo_alta,
        operacao_internacao_id,
        paciente_nome,
        municipio_paciente,
        paciente_dataNacimento,
        municipio_paciente_codigo_ibge,
        central_regulacao,
        unidade_origem,
        cnes_unidade_origem,
        municipio_unidade_origem_codigo_ibge,
        unidade_executante,
        cnes_unidade_executante,
        municipio_unidade_executante_codigo_ibge,
        motivo_cancelamento_solicitacao,
        especialidade,
        codigo_cid,
        numero_cid,
        procedimento,
        cns,
        data_alta,
        carater_internacao,
        Sala_Vermelha,
        Infarto_agudo,
        Infarto_agudo_supraST,
        infartoAgudo_inicioSintomas,
        Infarto_agudo_trombolizado,
        data_extracao
    from {{ source('brutos_ser_metabase_staging', 'FATO_INTERNACAO') }}
    where data_particao = "2025-10-15"
),

transform as (
    select
        /*
        -- pk
        safe_cast(solicitacao_id as int) as id_solicitacao,

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
        ) as data_evento, -- significado??

        -- unidades envolvidas
        lpad(safe_cast(safe_cast({{ process_null("cnes_unidade_origem")}} as int) as string), '0', 7) as id_cnes_unidade_origem, -- mesma coisa que unidade solicitante?
        lpad(safe_cast(safe_cast({{ process_null("cnes_unidade_executante")}} as int) as string), '0', 7) as id_cnes_unidade_executante,        

        -- qualificacao do procedimento
        carater_internacao as internacao_carater,
        upper(trim({{ process_null("tipo_de_leito") }})) as leito_tipo,
        upper(trim({{ process_null("tipo_leito_regulado") }})) as leito_regulado_tipo,
        upper(trim({{process_null("especialidade")}})) as especialidade,
        upper(trim({{process_null("procedimento")}})) as procedimento,
        tipointernacao as internacao_tipo,

        -- estado da solicitacao
        estadoSolicitacao as solicitacao_estado,
        estadoAtual as solicitacao_estado_atual, -- qual a diferenca para estadoSolicitacao?
        estadoAnterior as solicitacao_estado_anterior,

        -- paciente
        lpad(safe_cast(safe_cast(cns as int) as string), '0', 15) as paciente_cns,
        {{clean_name_string(process_null("paciente_nome"))}} as paciente_nome,
        paciente_dataNacimento as paciente_data_nascimento, -- ver formato e parsear
        safe_cast(municipio_paciente_codigo_ibge as int) as id_paciente_municipio_ibge, -- checar quantidade de digitos
        */


        -- modelar as seguintes colunas nao comentadas (rodar no bq e ver o que tem)
        motivo_alta as alta_motivo,
        codigo_cid,
        numero_cid,

        motivo_cancelamento_solicitacao,
        Sala_Vermelha,
        Infarto_agudo,
        Infarto_agudo_supraST,
        infartoAgudo_inicioSintomas,
        Infarto_agudo_trombolizado,

        nAcaoJudicial,
        nrOrdemJudicial,
        juiz,
        pena,
        decisaoJuiz,
        prazo,
        reu,
        observacao,
        evento,

        /*
        -- ???
        operacao_internacao_id as id_operacao_internacao, -- o que é isso?

        safe_cast({{ process_null("data_extracao")}} as timestamp) as data_extracao
        */
    from source
)


select *
from transform
where
    data_internacao_termino is not null
    and data_alta is not null
    and data_evento is not null
    and procedimento is not null
    and especialidade is not null
    and leito_tipo is not null
    and leito_regulado_tipo is not null
    and internacao_tipo is not null
    and solicitacao_estado is not null
    and solicitacao_estado_anterior is not null
    and solicitacao_estado_atual is not null
    and internacao_carater is not null
    and id_operacao_internacao is not null

    