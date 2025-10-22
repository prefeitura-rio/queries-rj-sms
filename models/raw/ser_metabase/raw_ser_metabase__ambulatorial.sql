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
    cluster_by=['id_cnes_unidade_origem', 'id_cnes_unidade_executante', 'procedimento_regulado', 'paciente_cns'],
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
        lpad(cast(safe_cast(split({{ process_null("unidade_origem_cnes") }}, '.')[0] as int) as string), 7, '0') as id_cnes_unidade_origem,
        lpad(cast(safe_cast(split({{ process_null("unidade_executante_cnes") }}, '.')[0] as int) as string), 7, '0') as id_cnes_unidade_executante,

        -- qualificacao do procedimento
        {{ process_null("classificacao_risco") }} as carater,
        upper(trim({{process_null("tipo_recurso_solicitado")}})) as procedimento_solicitado_tipo,
        upper(trim({{process_null("especialidade_solicitante")}})) as especialidade_solicitado,
        upper(trim({{process_null("recurso_solicitado")}})) as procedimento_solicitado,
        upper(trim({{process_null("tipo_recurso_regulado")}})) as procedimento_regulado_tipo,
        upper(trim({{process_null("especialidade_regulado")}})) as especialidade_regulado,
        upper(trim({{process_null("recurso_regulado")}})) as procedimento_regulado,
        upper(trim({{ process_null("codigo_cid") }})) as cid,
        cod_recurso_solicitado, --- cod procedimento no ser??
        cod_recurso_regulado, --- cod procedimento no ser??

        -- estado da solicitacao
        estado_solicitacao as solicitacao_estado,
        prioridade, -- 1.0, 2.0, etc.. qual o significado / mappinng? qual é a maior prioridade (maior ou menor)?
        motivo_cancelamento_solicitacao as justificativa_cancelamento,

        -- paciente
        lpad(cast(safe_cast(split(cns, '.')[0] as int) as string), 15, '0') as paciente_cns,
        {{clean_name_string(process_null("nome_paciente"))}} as paciente_nome,
        upper(trim({{ process_null("municipio_paciente") }})) as paciente_municipio,
        if(sexo_paciente = 'M','MASCULINO', if(sexo_paciente = 'F','FEMININO', NULL)) as paciente_sexo,
        coalesce(
            safe_cast({{ process_null("data_nascimento") }} as date),
            safe_cast({{ process_null("datanascimento") }} as date)
        ) as paciente_data_nascimento,

        -- indicadores
        if(apto_ao_tratamento = 'False','NAO', if(apto_ao_tratamento = 'True','SIM', NULL)) as indicador_apto_tratamento, -- entender melhor a semantica disto

        -- judicializacao
        nacaojudicial as jud_acao_judicial_id,
        {{ process_null(clean_name_string("juiz")) }} as jud_juiz_nome,
        decisaoJuiz as jud_decisao_juiz,
        pena as jud_pena,
        reu as jud_reu,
        mandado_judicial,
        prazo as jud_prazo,

        -- metadados 
        safe_cast({{ process_null("data_extracao")}} as timestamp) as data_extracao
    from source
),

final as (
    select
        *,    
        row_number() over (partition by id_solicitacao order by data_atualizacao_registro desc) as row_num
    from transform
    qualify row_num = 1
)

select
    * except (row_num)
from final 

