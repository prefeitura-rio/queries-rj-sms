{{
    config(
        alias='serie_temporal_atendimentos_prontua_rio',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário ProntuaRio'
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

-- Utilizei a tabela evolução ao invés de triagem porque há unidades que não possuem passagem pela triagem (Ex. Casa de Parto David Capistrano), 
-- mas possuem registro de evolução. A tabela evolução é mais abrangente e garante que todos os atendimentos sejam contabilizados.
-- Vale confirmar essa abordagem com o time do prontuário

with 
atendimentos_diarios as (
    select 
        date(registro_data) as data_dia,
        extract(dayofweek from registro_data) as dia_semana_num,
        format_date('%A', date(registro_data)) as dia_semana_nome,
        count(*) as atendimentos_no_dia
    from {{ ref('raw_prontuario_prontuaRio__evolucao') }}
    {% if is_incremental() %}
    where registro_data >= {{ partitions_to_replace }}
    {% endif %}   
    group by data_dia, dia_semana_num, dia_semana_nome
),
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