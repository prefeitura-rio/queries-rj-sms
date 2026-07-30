{{
    config(
        alias="atendimento",
        schema="projeto_ipp",
        materialized="incremental",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}


{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

with 
  atendimento as (
    select
      cpf,
      cnes_unidade,
      nome_equipe_profissional,
      tipo_consulta,
      {{ parse_and_filter_future_datetime('datahora_inicio') }} as datahora_inicio,
      datahora_fim,
      updated_at,
      loaded_at,
      data_particao
    from {{ ref('raw_prontuario_vitacare__atendimento') }}
    {% if is_incremental() %}
      where datahora_inicio >= {{ partitions_to_replace }}
    {% endif %}
  )

select 
   cpf,
   cnes_unidade as id_cnes,
   nome_equipe_profissional,
   tipo_consulta,
   datahora_inicio as inicio_datahora,
   datahora_fim as fim_datahora,

   -- Metadados
   loaded_at as carregado_em,
   updated_at as atualizado_em,
   current_datetime('America/Sao_Paulo') as processado_em,
  
   coalesce(
    safe_cast(datahora_inicio as date),
    safe_cast(datahora_fim as date)
   ) as data_particao,
   safe_cast(cpf as int64) as cpf_particao
from atendimento
where 
    {{ parse_and_filter_future_datetime('datahora_inicio') }} is not null
    and {{ parse_and_filter_future_datetime('datahora_fim') }} is not null
