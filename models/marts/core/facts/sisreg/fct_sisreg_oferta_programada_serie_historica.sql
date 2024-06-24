{{
    config(
        schema="saude_sisreg",
        alias="oferta_programada_serie_historica",
        materialized="incremental",
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
        from {{ ref("raw_sisreg__oferta_programada") }}
        where escala_status != "EXCLUIDA"
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
            id_escala_ambulatorial,
            id_central_executante,
            id_estabelecimento_executante,
            id_procedimento_interno,
            id_procedimento_unificado,
            id_cbo2002,
            profissional_executante_cpf,
            procedimento_vigencia_inicial_data,
            procedimento_vigencia_final_data,
            data as procedimento_vigencia_data,
            procedimento_dia_semana_sigla,
            extract(year from data) as procedimento_vigencia_ano,
            extract(month from data) as procedimento_vigencia_mes,
            data_particao,
            sum(vagas_primeira_vez_qtd) as vagas_primeira_vez_qtd,
            sum(vagas_reserva_qtd) as vagas_reserva_qtd,
            sum(vagas_retorno_qtd) as vagas_retorno_qtd,
            sum(
                vagas_primeira_vez_qtd + vagas_reserva_qtd + vagas_retorno_qtd
            ) as vagas_todas_qtd,
        from sisreg_explodido_filtrado
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
    )

select
    -- pk
    id_escala_ambulatorial,

    -- fks
    id_central_executante,
    id_estabelecimento_executante,
    id_procedimento_interno,
    id_procedimento_unificado,
    id_cbo2002,
    profissional_executante_cpf,

    -- common fields
    procedimento_vigencia_inicial_data,
    procedimento_vigencia_final_data,
    procedimento_vigencia_data,
    procedimento_vigencia_ano,
    procedimento_vigencia_mes,
    procedimento_dia_semana_sigla as procedimento_vigencia_dia_semana,
    vagas_primeira_vez_qtd,
    vagas_reserva_qtd,
    vagas_retorno_qtd,
    vagas_todas_qtd,

    -- metadados
    data_particao
from final
{% if is_incremental() %}

    where data_particao > (select max(data_particao) from {{ this }})

{% endif %}
