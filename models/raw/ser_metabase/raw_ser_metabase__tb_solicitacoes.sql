-- Contagem inicial de colunas: 29
{{
    config(
        schema="brutos_ser_metabase",
        alias="tb_solicitacoes",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    source as (

        select
            -- INFORMAÇÕES DA SOLICITAÇÃO
            safe_cast(data_solicitacao as date) as data_solicitacao,
            solicitacao_id,  -- entender como tratar
            safe_cast(dt_reserva as date) as dt_reserva,
            {{ clean_name_string("estadosolicitacao") }} as estadosolicitacao,
            safe_cast(data_evento_desistencia as date) as data_evento_desistencia,
            nacaojudicial,  -- entender como tratar

            -- INFORMAÇÕES DO PACIENTE
            {{ clean_name_string("paciente_nome") }} as paciente_nome,
            lpad(cns, 15, '0') as cns,  -- padding de CNS, se aplicável
            safe_cast(paciente_datanacimento as date) as paciente_datanacimento,
            {{ clean_name_string("municipio_paciente") }} as municipio_paciente,
            {{ clean_name_string("infarto_agudo") }} as infarto_agudo,

            -- CARACTERISTICAS DO LEITO E INTERNAÇÃO
            {{ clean_name_string("tipo_de_leito") }} as tipo_de_leito,
            {{ clean_name_string("carater_internacao") }} as carater_internacao,
            {{ clean_name_string("tipointernacao") }} as tipointernacao,

            -- PROCEDIMENTO E ESPECIALIDADE
            {{ clean_name_string("procedimento") }} as procedimento,
            {{ clean_name_string("especialidade") }} as especialidade,
            {{ clean_name_string("numero_cid") }} as numero_cid,

            -- UNIDADES (ORIGEM E EXECUTANTE)
            {{ clean_name_string("unidade_origem") }} as unidade_origem,
            lpad(cnes_unidade_origem, 7, '0') as cnes_unidade_origem,
            {{ clean_name_string("municipio_unidade_origem") }}
            as municipio_unidade_origem,

            {{ clean_name_string("unidade_executante") }} as unidade_executante,
            lpad(cnes_unidade_executante, 7, '0') as cnes_unidade_executante,
            {{ clean_name_string("municipio_unidade_executante") }}
            as municipio_unidade_executante,

            -- OUTRAS INFORMAÇÕES E CANCELAMENTOS
            {{ clean_name_string("motivo_cancelamento_solicitacao") }}
            as motivo_cancelamento_solicitacao,

            -- CENTRAL DE REGULAÇÃO
            {{ clean_name_string("central_regulacao") }} as central_regulacao,

            -- EXTRAÇÃO E PARTICIONAMENTO
            safe_cast(data_extracao as datetime) as data_extracao,
            safe_cast(ano_particao as int64) as ano_particao,
            safe_cast(mes_particao as int64) as mes_particao,
            safe_cast(data_particao as date) as data_particao

        from {{ source("brutos_ser_metabase_staging", "TB_SOLICITACOES") }}
    )

select *
from source
