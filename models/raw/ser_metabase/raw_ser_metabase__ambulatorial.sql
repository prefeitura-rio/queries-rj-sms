-- noqa: disable=LT08
{{
  config(
    enabled=true,
    schema="ser_matabase",
    alias="ambulatorial",
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id_solicitacao',
    partition_by={
      "field": "data_solicitacao",
      "data_type": "date",
      "granularity": "month",
    },
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
        dt_solicitacao, 
        nome_paciente, 
        municipio_paciente, 
        cns,  
        prioridade,  
        nacaojudicial, 
        juiz, 
        decisaojuiz, 
        pena, 
        reu, 
        prazo, 
        classificacao_risco,
        data_nascimento, 
        unidade_origem_id, 
        hospital_origem_nao_identificado, 
        unidadeidentificada, 
        codigo_cid, 
        recurso_solicitado, 
        tipo_recurso_solicitado, 
        cod_recurso_solicitado, 
        recurso_regulado, 
        tipo_recurso_regulado, 
        cod_recurso_regulado, 
        estado_solicitacao, 
        mandado_judicial, 
        data, 
        apto_ao_tratamento, 
        classificacao_risco_alterada, 
        unidade_executante_id, 
        dt_agendamento, 
        dt_execucao, 
        data_prevista_tratamento, 
        dt_inicio_efetiva_tratamento, 
        recurso_solicitado_sisreg, 
        recurso_regulado_sisreg,  
        motivo_cancelamento_solicitacao, 
        datanascimento, 
        sexo_paciente, 
        especialidade_solicitante, 
        especialidade_regulado, 
        unidade_origem_cnes, 
        unidade_executante_cnes, 
        data_extracao 

    from {{ source('brutos_ser_metabase_staging', 'FATO_AMBULATORIO') }} 
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
            {{ process_null("dt_solicitacao") }} as timestamp), 'America/Sao_Paulo'
        ) as data_solicitacao,

        date(safe_cast(
            {{ process_null("dt_agendamento") }} as timestamp), 'America/Sao_Paulo'   
        ) as data_agendamento,

        date(safe_cast(
            {{ process_null("dt_execucao") }} as timestamp), 'America/Sao_Paulo'   
        ) as data_execucao,        

        date(safe_cast(
            {{ process_null("dt_inicio_efetiva_tratamento") }} as timestamp), 'America/Sao_Paulo'
        ) as data_tratamento_inicio,

        date(safe_cast(
            {{ process_null("data_prevista_tratamento") }} as timestamp), 'America/Sao_Paulo'
        ) as data_tratamento_prevista,

        date(safe_cast(
            {{ process_null("data") }} as timestamp), 'America/Sao_Paulo'
        ) as data_atualizacao_registro,

        -- unidades envolvidas
        unidade_origem_id,
        unidade_origem_cnes,
        hospital_origem_nao_identificado,
        unidadeidentificada,
        unidade_executante_id,
        unidade_executante_cnes,

        -- qualificacao do procedimento
        recurso_solicitado,
        tipo_recurso_solicitado,
        cod_recurso_solicitado,
        recurso_regulado,
        cod_recurso_regulado,
        tipo_recurso_regulado,
        codigo_cid,
        especialidade_solicitante,
        especialidade_regulado,
        recurso_solicitado_sisreg,
        recurso_regulado_sisreg,  

        -- estado da solicitacao
        classificacao_risco,
        prioridade,
        estado_solicitacao,
        classificacao_risco_alterada,
        motivo_cancelamento_solicitacao,

        -- paciente 
        nome_paciente, 
        municipio_paciente,
        cns,
        sexo_paciente,
        safe_cast({{ process_null("data_nascimento") }} as date) as paciente_data_nascimento,
        safe_cast({{ process_null("datanascimento") }} as date) as paciente_data_nascimento_2,

        -- indicadores
        apto_ao_tratamento,

        -- judicializacao
        nacaojudicial,
        juiz,
        decisaojuiz,
        pena,
        reu,
        mandado_judicial,
        prazo,

        -- metadados 
        safe_cast({{ process_null("data_extracao")}} as timestamp) as data_extracao

    from source
)

select * from transform
