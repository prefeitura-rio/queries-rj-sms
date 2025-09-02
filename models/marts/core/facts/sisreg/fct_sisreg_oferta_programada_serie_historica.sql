{{
    config(
        schema="saude_sisreg",
        alias="oferta_programada_serie_historica",
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

{% set last_partition = get_last_partition_date( this ) %}

with
    -- sources
    sisreg as (
        select *
        from {{ ref("raw_sisreg__oferta_programada") }}
        where escala_status != "EXCLUIDA"
        {% if is_incremental() %}
        and data_particao >= '{{ last_partition }}'
        {% endif %}
    ),

    nomes_procedimentos as (
        select id_procedimento, descricao as procedimento
        from {{ ref("raw_sheets__assistencial_procedimento") }}
    ),

    nomes_estabelecimentos as (
        select id_cnes, nome_fantasia as estabelecimento
        from {{ ref("raw_sheets__estabelecimento_auxiliar") }}
    ),

    ocupacoes as (
        select distinct id_cbo, upper(descricao) as ocupacao
        from {{ ref("raw_datasus__cbo") }}
    ),

    ocupacoes_familia as (
        select distinct id_cbo_familia, upper(descricao) as ocupacao_familia
        from {{ ref("raw_datasus__cbo_fam") }}
    ),

    sisreg_enriquecido as (
        select
            id_escala_ambulatorial,
            id_central_executante,
            id_estabelecimento_executante,
            estabelecimento,
            id_procedimento_interno,
            id_procedimento_unificado,
            procedimento,
            id_cbo2002,
            ocup.ocupacao,
            ocupf.ocupacao_familia,
            profissional_executante_cpf,
            profissional_executante_nome,
            procedimento_vigencia_inicial_data,
            procedimento_vigencia_final_data,
            procedimento_dia_semana_sigla,
            data_particao,
            sum(vagas_primeira_vez_qtd) as vagas_primeira_vez_qtd,
            sum(vagas_reserva_qtd) as vagas_reserva_qtd,
            sum(vagas_retorno_qtd) as vagas_retorno_qtd,
            sum(
                vagas_primeira_vez_qtd + vagas_reserva_qtd + vagas_retorno_qtd
            ) as vagas_todas_qtd,
        from sisreg as sef
        left join
            nomes_procedimentos as np
            on sef.id_procedimento_interno = np.id_procedimento
        left join
            nomes_estabelecimentos as ne
            on sef.id_estabelecimento_executante = ne.id_cnes
        left join ocupacoes as ocup on sef.id_cbo2002 = ocup.id_cbo
        left join
            ocupacoes_familia as ocupf on left(sef.id_cbo2002, 4) = ocupf.id_cbo_familia

        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
    ),
    sisreg_explodido_data as (
        select
            id_escala_ambulatorial,
            procedimento_dia_semana_sigla,
            data,
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
    estabelecimento,
    procedimento,
    procedimento_vigencia_inicial_data,
    procedimento_vigencia_final_data,
    data as procedimento_vigencia_data,
    extract(year from data) as procedimento_vigencia_ano,
    extract(month from data) as procedimento_vigencia_mes,
    sisreg_enriquecido.procedimento_dia_semana_sigla as procedimento_vigencia_dia_semana,
    ocupacao_familia,
    ocupacao,
    profissional_executante_nome,
    vagas_primeira_vez_qtd,
    vagas_reserva_qtd,
    vagas_retorno_qtd,
    vagas_todas_qtd,

    -- metadados
    data_particao

from sisreg_enriquecido
left join sisreg_explodido_filtrado
using (id_escala_ambulatorial)