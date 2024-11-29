-- Este modelo dbt identifica profissionais que estão oferecendo significativamente
-- menos vagas
-- em comparação com seus pares, ajustado pela carga horária e distribuição das
-- vagas planejadas
-- entre diferentes procedimentos.
{{
    config(
        enabled=true,
        schema="projeto_sisreg_reports",
        alias="oferta_programada__anomalia_procedimento",
        materialized="incremental",
        partition_by={"field": "data_calculo_anomalia", "data_type": "DATE"},
    )
}}

{% set data_atual = run_started_at.strftime("%Y-%m-%d") %}

-- Passo 1: Identificar a data de partição mais recente em
-- 'fct_sisreg_oferta_programada_serie_historica'
with
    latest_partition_date as (
        select max(data_particao) as max_partition_date
        from {{ ref("fct_sisreg_oferta_programada_serie_historica") }}
    ),

    -- Passo 2: Definir os intervalos de tempo relevantes
    time_intervals as (
        select
            date_trunc(
                date_add(date('{{ data_atual }}'), interval 1 month), month
            ) as next_month_start,
            date_trunc(
                date_add(date('{{ data_atual }}'), interval 2 month), month
            ) as next_month_end
    ),

    -- Passo 3: Obter os profissionais mais recentes com carga horária ambulatorial
    latest_professionals as (
        select id_cnes, cpf, carga_horaria_ambulatorial
        from {{ ref("dim_profissional_sus_rio_historico") }}

        where
            cpf is not null
            and carga_horaria_ambulatorial is not null
            and carga_horaria_ambulatorial > 0
            and (ano_competencia, mes_competencia) = (
                select as struct ano_competencia, mes_competencia
                from {{ ref("dim_profissional_sus_rio_historico") }}
                order by ano_competencia desc, mes_competencia desc
                limit 1
            )
            and data_particao = (
                select max(data_particao)
                from {{ ref("dim_profissional_sus_rio_historico") }}
            )
    ),

    -- Passo 4: Obter as vagas programadas para o próximo mês
    next_month_vacancies as (
        select
            profissional_executante_cpf as profissional_cpf,
            id_procedimento_interno as procedimento_id,
            id_estabelecimento_executante as estabelecimento_id,
            date_trunc(procedimento_vigencia_data, month) as competencia,
            sum(vagas_todas_qtd) as total_vacancies_next_month
        from {{ ref("fct_sisreg_oferta_programada_serie_historica") }}
        where
            data_particao = (select max_partition_date from latest_partition_date)
            and procedimento_vigencia_data
            >= (select next_month_start from time_intervals)
            and procedimento_vigencia_data < (select next_month_end from time_intervals)
        group by profissional_cpf, procedimento_id, estabelecimento_id, competencia
    ),

    -- Passo 5: Calcular o total de vagas programadas por profissional no
    -- estabelecimento
    total_vacancies_per_professional as (
        select
            profissional_cpf,
            estabelecimento_id,
            sum(total_vacancies_next_month) as total_vacancies_professional
        from next_month_vacancies
        group by profissional_cpf, estabelecimento_id
    ),

    -- Passo 6: Normalizar as vagas pela carga horária ambulatorial usando a
    -- distribuição real
    normalized_vacancies as (
        select
            nmv.*,
            lp.carga_horaria_ambulatorial,
            tvp.total_vacancies_professional,

            -- Transformação das vagas
            ln(
                1 + (
                    round(
                        pow(nmv.total_vacancies_next_month, 2) / (
                            lp.carga_horaria_ambulatorial
                            * tvp.total_vacancies_professional
                        ),
                        2
                    )
                )
            ) as transformed_adjusted_vacancies_per_ch

        from next_month_vacancies nmv
        left join
            latest_professionals lp
            on lp.id_cnes = nmv.estabelecimento_id
            and lp.cpf = nmv.profissional_cpf
        left join
            total_vacancies_per_professional tvp
            on nmv.profissional_cpf = tvp.profissional_cpf
            and nmv.estabelecimento_id = tvp.estabelecimento_id
        where
            lp.carga_horaria_ambulatorial is not null
            and tvp.total_vacancies_professional > 0
    ),

    -- Passo 7: Agregar as vagas normalizadas por procedimento e estabelecimento
    procedure_establishment_vacancies as (
        select
            competencia,
            procedimento_id,
            estabelecimento_id,
            array_agg(profissional_cpf) as profissionais,
            array_agg(carga_horaria_ambulatorial) as cargas_horarias,
            array_agg(total_vacancies_next_month) as total_vacancies,
            array_agg(
                transformed_adjusted_vacancies_per_ch
            ) as transformed_adjusted_vacancies_per_ch_list
        from normalized_vacancies
        where transformed_adjusted_vacancies_per_ch is not null
        group by competencia, procedimento_id, estabelecimento_id
        having array_length(profissionais) >= 3
    ),

    -- Passo 8: Calcular os quartis (Q1 e Q3) exatos para cada grupo
    quartiles as (
        select
            competencia,
            procedimento_id,
            estabelecimento_id,
            percentile_cont(transformed_vacancy, 0.25) over (
                partition by competencia, procedimento_id, estabelecimento_id
            ) as q1,
            percentile_cont(transformed_vacancy, 0.75) over (
                partition by competencia, procedimento_id, estabelecimento_id
            ) as q3
        from
            (
                select
                    competencia,
                    procedimento_id,
                    estabelecimento_id,
                    transformed_vacancy
                from
                    procedure_establishment_vacancies,
                    unnest(
                        transformed_adjusted_vacancies_per_ch_list
                    ) as transformed_vacancy
            )
        group by competencia, procedimento_id, estabelecimento_id, transformed_vacancy
    ),

    -- Passo 9: Calcular o Intervalo Interquartil (IQR) e o limite inferior
    iqr_calculations as (
        select distinct
            nv.*,
            q.q1,
            q.q3,
            (q.q3 - q.q1) as iqr,
            (q.q1 - 1.5 * (q.q3 - q1)) as lower_bound
        from normalized_vacancies nv
        join
            quartiles q
            on nv.competencia = q.competencia
            and nv.procedimento_id = q.procedimento_id
            and nv.estabelecimento_id = q.estabelecimento_id
    ),

    -- Passo 10: Seleção final: Identificar outliers abaixo do limite inferior
    final as (
        select
            iqr.competencia,
            iqr.estabelecimento_id as id_cnes,
            iqr.profissional_cpf,
            iqr.procedimento_id as id_procedimento,
            iqr.total_vacancies_next_month as vagas_programadas_competencia,
            iqr.carga_horaria_ambulatorial as carga_horaria_ambulatorial_semanal,
            pev.total_vacancies as vagas_colegas_cnes_proced,
            pev.cargas_horarias as ch_amb_colegas_cnes_proced,
            date('{{ data_atual }}') as data_calculo_anomalia
        from iqr_calculations iqr
        left join
            procedure_establishment_vacancies pev
            on iqr.procedimento_id = pev.procedimento_id
            and iqr.estabelecimento_id = pev.estabelecimento_id
        where iqr.transformed_adjusted_vacancies_per_ch < iqr.lower_bound
        order by
            id_cnes,
            profissional_cpf,
            id_procedimento,
            iqr.carga_horaria_ambulatorial desc
    )

select *
from final
{% if is_incremental() %}
    where data_calculo_anomalia > (select max(data_calculo_anomalia) from {{ this }})
{% endif %}
