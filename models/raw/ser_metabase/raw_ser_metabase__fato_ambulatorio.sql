{{
    config(
        schema="brutos_ser_metabase",
        alias="fato_ambulatorio",
        materialized="incremental",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}
{% set last_partition = get_last_partition_date( this ) %}
with
    source as (

        select
            -- INFORMAÇÕES DA SOLICITAÇÃO
            solicitacao_id,  -- entender como tratar
            safe_cast(dt_solicitacao as datetime) as dt_solicitacao,
            safe_cast(prioridade as int) as prioridade,
            safe_cast(rank as int) as rank,
            {{ clean_name_string("estado_solicitacao") }} as estado_solicitacao,
            safe_cast(apto_ao_tratamento as boolean) as apto_ao_tratamento,
            {{ clean_name_string("classificacao_risco") }} as classificacao_risco,
            safe_cast(
                classificacao_risco_alterada as boolean
            ) as classificacao_risco_alterada,
            safe_cast(data as datetime) as data,
            safe_cast(hora as time) as hora,
            {{ clean_name_string("nome") }} as nome,

            -- INFORMAÇÕES DO PACIENTE
            {{ clean_name_string("nome_paciente") }} as nome_paciente,
            {{ clean_name_string("municipio_paciente") }} as municipio_paciente,
            lpad(cns, 15, '0') as cns,
            safe_cast(data_nascimento as datetime) as data_nascimento,
            safe_cast(datanascimento as datetime) as datanascimento,
            {{ clean_name_string("sexo_paciente") }} as sexo_paciente,

            -- UNIDADE DE ORIGEM
            unidade_origem_id,  -- entender como tratar
            lpad(unidade_origem_cnes, 7, '0') as unidade_origem_cnes,
            {{ clean_name_string("unidade_origem") }} as unidade_origem,
            {{ clean_name_string("municipio_unidade_origem") }}
            as municipio_unidade_origem,
            codigo_ibge_unidade_origem,  -- entender como tratar
            {{ clean_name_string("hospital_origem_nao_identificado") }}
            as hospital_origem_nao_identificado,
            safe_cast(unidadeidentificada as boolean) as unidadeidentificada,

            -- UNIDADE EXECUTANTE
            unidade_executante_id,  -- entender como tratar
            {{ clean_name_string("unidadeexecutante") }} as unidadeexecutante,
            {{ clean_name_string("municipio_unidade_executante") }}
            as municipio_unidade_executante,
            lpad(unidade_executante_cnes, 7, '0') as unidade_executante_cnes,

            -- CENTRAL DE REGULAÇÃO
            centralregulacao_id,  -- entender como tratar
            {{ clean_name_string("central_regulacao") }} as central_regulacao,

            -- CID E RECURSOS
            codigo_cid,  -- entender como tratar
            {{ clean_name_string("descricao_cid") }} as descricao_cid,
            {{ clean_name_string("recurso_solicitado") }} as recurso_solicitado,
            {{ clean_name_string("tipo_recurso_solicitado") }}
            as tipo_recurso_solicitado,
            cod_recurso_solicitado,  -- entender como tratar
            {{ clean_name_string("recurso_regulado") }} as recurso_regulado,
            {{ clean_name_string("tipo_recurso_regulado") }} as tipo_recurso_regulado,
            cod_recurso_regulado,  -- entender como tratar
            {{ clean_name_string("recurso_solicitado_sisreg") }}
            as recurso_solicitado_sisreg,
            {{ clean_name_string("recurso_regulado_sisreg") }}
            as recurso_regulado_sisreg,
            {{ clean_name_string("especialidade_solicitante") }}
            as especialidade_solicitante,
            {{ clean_name_string("especialidade_regulado") }} as especialidade_regulado,

            -- DATAS DE AGENDAMENTO/EXECUÇÃO
            safe_cast(dt_agendamento as datetime) as dt_agendamento,
            safe_cast(dt_execucao as datetime) as dt_execucao,
            safe_cast(data_prevista_tratamento as datetime) as data_prevista_tratamento,
            safe_cast(
                dt_inicio_efetiva_tratamento as datetime
            ) as dt_inicio_efetiva_tratamento,

            -- CANCELAMENTO
            {{ clean_name_string("motivo_cancelamento_solicitacao") }}
            as motivo_cancelamento_solicitacao,

            -- INFORMAÇÕES JUDICIAIS
            {{ clean_name_string("mandado_judicial") }} as mandado_judicial,
            nacaojudicial,  -- entender como tratar
            {{ clean_name_string("juiz") }} as juiz,
            decisaojuiz,  -- entender como tratar
            pena,  -- entender como tratar
            {{ clean_name_string("reu") }} as reu,
            prazo,  -- entender como tratar

            -- METADADOS EXTRAÇÃO E PARTIÇÃO
            safe_cast(data_extracao as datetime) as data_extracao,
            safe_cast(ano_particao as int64) as ano_particao,
            safe_cast(mes_particao as int64) as mes_particao,
            safe_cast(data_particao as date) as data_particao

        from {{ source("brutos_ser_metabase_staging", "FATO_AMBULATORIO") }}
        {% if is_incremental() %}
        where data_particao > '{{ last_partition }}'
        {% endif %}

    )

select *
from source
