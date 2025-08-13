{{
    config(
        schema="brutos_sisreg",
        alias="oferta_programada",
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
        select * 
        from {{ source("brutos_sisreg_staging", "escala") }}
        {% if is_incremental() %}

        where data_particao > '{{ last_partition }}'
        {% endif %}
    ),
    renamed as (
        select
            cod_escala_ambulatorial as id_escala_ambulatorial,
            cod_central_exec as id_central_executante,
            desc_central_exec as central_executante_nome,
            lpad(cpf_profissional_exec, 11, "0") as profissional_executante_cpf,
            nome_profissional_exec as profissional_executante_nome,
            if(
                cod_cbo = "---" or cod_cbo = "", null, lpad(cod_cbo, 6, "0")
            ) as id_cbo2002,
            if(desc_cbo = "---" or desc_cbo = "", null, desc_cbo) as cbo2002_descricao,
            lpad(cod_cnes_exec, 7, "0") as id_estabelecimento_executante,
            desc_cnes_exec as estabelecimento_executante_nome,
            lpad(cod_procedimento_interno, 7, "0") as id_procedimento_interno,
            desc_procedimento_interno as procedimento_interno_descricao,
            cod_procedimento_unificado as id_procedimento_unificado,
            sigla_dia_semana as procedimento_dia_semana_sigla,
            safe_cast(qtd_vagas_prim_vez as int64) as vagas_primeira_vez_qtd,
            safe_cast(
                qtd_minutos_prim_vez as int64
            ) as vagas_primeira_vez_minutos_por_procedimento,
            safe_cast(qtd_vagas_retorno as int64) as vagas_retorno_qtd,
            safe_cast(
                qtd_minutos_retorno as int64
            ) as vagas_retorno_minutos_por_procedimento,
            safe_cast(qtd_vagas_reserva as int64) as vagas_reserva_qtd,
            safe_cast(
                qtd_minutos_reserva as int64
            ) as vagas_reserva_minutos_por_procedimento,
            quebra_automatica,
            agenda_local,
            if(
                data_de_vigencia_inicial != "" and data_de_vigencia_inicial != "---",
                parse_date('%d/%m/%Y', data_de_vigencia_inicial),
                null
            ) as procedimento_vigencia_inicial_data,
            if(
                data_de_vigencia_final != "" and data_de_vigencia_final != "---",
                parse_date('%d/%m/%Y', data_de_vigencia_final),
                null
            ) as procedimento_vigencia_final_data,
            hora_inicial as procedimento_hora_inicial,
            hora_final as procedimento_hora_final,
            nome_operador_criador as operador_nome_criador,
            nome_operador_modificador as operador_nome_modificador,
            if(
                data_ultima_alteracao != "" and data_ultima_alteracao != "---",
                parse_date('%d/%m/%Y', data_ultima_alteracao),
                null
            ) as escala_data_ultima_alteracao,
            status as escala_status,
            if(
                data_da_insercao != "" and data_da_insercao != "---",
                parse_date('%d/%m/%Y', data_da_insercao),
                null
            ) as escala_data_insercao,
            hora_da_insercao as escala_hora_insercao,
            if(
                data_da_ultima_ativacao != "" and data_da_ultima_ativacao != "---",
                parse_date('%d/%m/%Y', data_da_ultima_ativacao),
                null
            ) as escala_data_ultima_ativacao,
            hora_da_ultima_ativacao as escala_hora_ultima_ativacao,
            _data_carga,
            ano_particao,
            mes_particao,
            safe_cast(data_particao as date) as data_particao,

        from source
    )
select
    -- pk
    id_escala_ambulatorial,
    -- fk
    id_central_executante,
    id_estabelecimento_executante,
    id_procedimento_interno,
    id_procedimento_unificado,
    id_cbo2002,
    profissional_executante_cpf,

    -- dados gerais
    central_executante_nome,
    estabelecimento_executante_nome,
    procedimento_interno_descricao,
    cbo2002_descricao,
    profissional_executante_nome,
    procedimento_vigencia_inicial_data,
    procedimento_vigencia_final_data,
    procedimento_dia_semana_sigla,
    procedimento_hora_inicial,
    procedimento_hora_final,
    escala_status,
    vagas_primeira_vez_qtd,
    vagas_primeira_vez_minutos_por_procedimento,
    vagas_retorno_qtd,
    vagas_retorno_minutos_por_procedimento,
    vagas_reserva_qtd,
    vagas_reserva_minutos_por_procedimento,

    -- metadados
    agenda_local,
    quebra_automatica,
    escala_data_insercao,
    escala_hora_insercao,
    escala_data_ultima_ativacao,
    escala_hora_ultima_ativacao
    escala_data_ultima_alteracao,
    operador_nome_criador,
    operador_nome_modificador,
    _data_carga,
    ano_particao,
    mes_particao,
    data_particao
from renamed
