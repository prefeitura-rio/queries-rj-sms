{{
    config(
        alias="escala",
        schema="saude_sisreg",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    -- sources
    sisreg as (
        select *
        from {{ ref("raw_sisreg__escala") }}
        where
            -- - data_particao = current_date('America/Sao_Paulo')
            escala_status != "EXCLUIDA"
    ),

    sisreg_explodido_data as (
        select
            *,
            case
                extract(dayofweek from data)
                when 1
                then 'DOM'
                when 2
                then 'SEG'
                when 3
                then 'TER'
                when 4
                then 'QUA'
                when 5
                then 'QUI'
                when 6
                then 'SEX'
                when 7
                then 'SAB'
            end as dia_semana_verdadeiro,
        from
            sisreg,
            unnest(
                generate_date_array(
                    procedimento_vigencia_inicial_data, procedimento_vigencia_final_data
                )
            ) as data
    ),

    sisreg_explodido_filtrado as (
        select *
        from sisreg_explodido_data
        where dia_semana_verdadeiro = procedimento_dia_semana_sigla
    ),
    final as (
        select
            -- - pk
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
            data as procedimento_vigencia_data,
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
            escala_data_ultima_ativacao,
            escala_data_ultima_alteracao,
            operador_nome_criador,
            operador_nome_modificador,
            _data_carga,
            ano_particao,
            mes_particao,
            data_particao
        from sisreg_explodido_filtrado
        where data_particao = current_date('America/Sao_Paulo')
        order by
            id_escala_ambulatorial,
            id_central_executante,
            id_estabelecimento_executante,
            id_procedimento_interno,
            id_procedimento_unificado,
            id_cbo2002,
            profissional_executante_cpf,
            central_executante_nome,
            estabelecimento_executante_nome,
            procedimento_interno_descricao,
            cbo2002_descricao,
            profissional_executante_nome,
            procedimento_vigencia_inicial_data,
            procedimento_vigencia_final_data,
            data,
            procedimento_dia_semana_sigla,
            procedimento_hora_inicial,
            procedimento_hora_final,
            escala_status
    )

select
    id_central_executante,
    id_estabelecimento_executante,
    id_procedimento_interno,
    id_procedimento_unificado,
    id_cbo2002,
    profissional_executante_cpf,
    procedimento_vigencia_inicial_data,
    procedimento_vigencia_final_data,
    procedimento_vigencia_data,
    extract(year from procedimento_vigencia_data) as procedimento_vigencia_ano,
    extract(month from procedimento_vigencia_data) as procedimento_vigencia_mes,
    sum(vagas_primeira_vez_qtd) as vagas_primeira_vez_qtd,
    sum(vagas_reserva_qtd) as vagas_reserva_qtd,
    sum(vagas_retorno_qtd) as vagas_retorno_qtd,
    sum(
        vagas_primeira_vez_qtd + vagas_reserva_qtd + vagas_retorno_qtd 
    ) as vagas_todas_qtd,
from final
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
