{{
    config(
        alias='serie_temporal_atendimentos_vitacare',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário Vitacare'
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

with atendimentos_diarios as (
    select 
        date(datahora_inicio) as data_dia,
        extract(dayofweek from datahora_inicio) as dia_semana_num,
        format_date('%A', date(datahora_inicio)) as dia_semana_nome,
        count(*) as atendimentos_no_dia
    from {{ ref('raw_prontuario_vitacare__atendimento') }}
    {% if is_incremental() %}
    -- precisamos de 30 dias extras de histórico (60 no total) para poder calcular
    -- a mediana móvel de cada uma das datas que serão reprocessadas
    where datahora_inicio >= date_sub({{ partitions_to_replace }}, interval 30 day)
    {% endif %}   
    group by data_dia, dia_semana_num, dia_semana_nome
),

-- para cada data, calcula a mediana de atendimentos do mesmo dia da semana
-- considerando apenas os 30 dias anteriores a ela (janela móvel)
mediana_movel as (
    select
        a.data_dia,
        percentile_cont(b.atendimentos_no_dia, 0.5) over (
            partition by a.data_dia
        ) as mediana_ultimos_30_dias
    from atendimentos_diarios a
    join atendimentos_diarios b
        on b.dia_semana_num = a.dia_semana_num
        and b.data_dia between date_sub(a.data_dia, interval 30 day) 
                           and date_sub(a.data_dia, interval 1 day)
    {% if is_incremental() %}
    where a.data_dia >= {{ partitions_to_replace }}
    {% endif %}
),

mediana_dia_semana as (
    select distinct
        data_dia,
        mediana_ultimos_30_dias
    from mediana_movel
),

final as (
    select 
        d.data_dia as data_registro,
        case d.dia_semana_nome
            when 'Monday' then 'Segunda'
            when 'Tuesday' then 'Terça'
            when 'Wednesday' then 'Quarta'
            when 'Thursday' then 'Quinta'
            when 'Friday' then 'Sexta'
            when 'Saturday' then 'Sábado'
            when 'Sunday' then 'Domingo'
        end as dia_semana_nome,
        d.atendimentos_no_dia,
        m.mediana_ultimos_30_dias
    from atendimentos_diarios d
    join mediana_dia_semana m 
        on d.data_dia = m.data_dia
    {% if is_incremental() %}
    where d.data_dia >= {{ partitions_to_replace }}
    {% endif %}
    order by 
        d.data_dia
)

select * from final