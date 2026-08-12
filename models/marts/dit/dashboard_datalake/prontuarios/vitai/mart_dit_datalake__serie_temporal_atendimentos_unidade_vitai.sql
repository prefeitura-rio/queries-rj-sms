{{
    config(
        alias='serie_temporal_atendimentos_unidade_vt',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['cnes', 'nome', 'data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário Vitai, segmentada por unidade de saúde'
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

with atendimentos_diarios as (
    select 
        date(data_entrada) as data_dia,
        cnes as id_cnes,
        extract(dayofweek from data_entrada) as dia_semana_num,
        format_date('%A', date(data_entrada)) as dia_semana_nome,
        count(*) as atendimentos_no_dia
    from {{ ref('raw_prontuario_vitai__boletim') }} b
    left join {{ ref('raw_prontuario_vitai__m_estabelecimento')}} e
      on b.gid_estabelecimento = e.gid
    {% if is_incremental() %}
    where data_entrada >= {{ partitions_to_replace }}
    {% endif %}   
    group by data_dia, id_cnes, dia_semana_num, dia_semana_nome
),
mediana_movel as (
    select
        a.data_dia,
        a.id_cnes,
        percentile_cont(b.atendimentos_no_dia, 0.5) over (
            partition by a.data_dia, a.id_cnes
        ) as mediana_ultimos_30_dias
    from atendimentos_diarios a
    join atendimentos_diarios b
        on b.id_cnes = a.id_cnes
        and b.dia_semana_num = a.dia_semana_num
        and b.data_dia between date_sub(a.data_dia, interval 30 day) 
                           and date_sub(a.data_dia, interval 1 day)
    {% if is_incremental() %}
    where a.data_dia >= {{ partitions_to_replace }}
    {% endif %}
),
mediana_dia_semana as (
    select distinct
        data_dia,
        id_cnes,
        mediana_ultimos_30_dias
    from mediana_movel
),
estabelecimentos as (
  select 
    id_cnes, 
    nome_acentuado as nome
  from {{ref('dim_estabelecimento')}}
),
final as (
    select 
        {{ parse_and_filter_future_date('d.data_dia') }} as data_registro,
        d.id_cnes as cnes,
        case lower(e.nome) 
          when 'coord de emergência regional cer barra' then 'CER Barra'
          when 'coord de emergência regional cer leblon' then 'CER Leblon'
          when 'coord de emergência regional cer centro' then 'CER Centro'
          else {{proper_estabelecimento('e.nome')}}
        end as nome,
        case d.dia_semana_nome
            when 'Monday' then 'Segunda'
            when 'Tuesday' then 'Terça'
            when 'Wednesday' then 'Quarta'
            when 'Thursday' then 'Quinta'
            when 'Friday' then 'Sexta'
            when 'Saturday' then 'Sábado'
            when 'Sunday' then 'Domingo'
        end as dia_semana_nome,
        d.atendimentos_no_dia as atendimentos,
        m.mediana_ultimos_30_dias
    from atendimentos_diarios d
    join mediana_dia_semana m 
        on d.data_dia = m.data_dia
        and d.id_cnes = m.id_cnes
    join estabelecimentos e on e.id_cnes = d.id_cnes
    {% if is_incremental() %}
    where d.data_dia >= {{ partitions_to_replace }}
    {% endif %}
)

select * from final