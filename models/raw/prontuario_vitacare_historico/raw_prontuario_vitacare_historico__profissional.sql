{{
  config(
    alias="profissional",
    materialized="incremental",    
    incremental_strategy="merge",
    schema="brutos_prontuario_vitacare_historico",
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day"
    },
    unique_key=['id_global'],
    cluster_by=['id_global']
  )
}}

{% set last_partition = get_last_partition_date(this) %}

with
  source_profissionais as (
    select 
      *
    from {{ source('brutos_prontuario_vitacare_historico_staging', 'profissionais') }} 
    {% if is_incremental() %}
      where data_particao > '{{last_partition}}'
    {% endif %}
  ),

  dedup_profissionais as (
       select
           *
       from source_profissionais 
       qualify row_number() over (partition by id_cnes, prof_id order by extracted_at desc) = 1
   ),

  fato_profissionais as (
    select
      concat(id_cnes, '.', replace(prof_id, '.0', '')) as id_global,
      replace(prof_id, '.0', '') as id_local,
      
      {{ process_null('profissional_cns') }} as profissional_cns,
      {{ process_null('profissional_cpf') }} as profissional_cpf,
      {{ process_null(proper_br('profissional_nome')) }} as profissional_nome,
      {{ process_null('n_registro') }} as n_registro,
      {{ process_null('profissional_cbo') }} as profissional_cbo,
      {{ process_null('profissional_cbo_descricao') }} as profissional_cbo_descricao,
      {{ process_null('profissional_equipe_nome') }} as profissional_equipe_nome,
      {{ process_null('profissional_equipe_cod_equipe') }} as profissional_equipe_cod_equipe,
      {{ process_null('profissional_equipe_cod_ine') }} as profissional_equipe_cod_ine,
   
      cast({{ process_null('extracted_at') }} as datetime) as loaded_at,
      date(cast(extracted_at as datetime)) as data_particao

    from dedup_profissionais
  )

select *
from fato_profissionais