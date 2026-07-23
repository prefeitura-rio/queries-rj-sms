{{
    config(
        alias='serie_temporal_atendimentos_unidade_prontua_rio',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "day"
        },
        unique_key=['cnes', 'data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário ProntuaRio, segmentada por unidade de saúde',
        tags=['datalake']
    )
}}

{% set last_partition = get_last_partition_date(this) %}

-- Utilizei a tabela evolução ao invés de triagem porque há unidades que não possuem passagem pela triagem (Ex. Casa de Parto David Capistrano), 
-- mas possuem registro de evolução. A tabela evolução é mais abrangente e garante que todos os atendimentos sejam contabilizados.
-- Vale confirmar essa abordagem com o time do prontuário

with
    atendimento_unidade as(
      select
        cnes as id_cnes,
        {{ parse_and_filter_future_date('registro_data') }} as data_registro,
        count(distinct gid_prontuario) as atendimentos
      from {{ ref('raw_prontuario_prontuaRio__evolucao') }}
      group by 1,2
      {% if is_incremental() %}
          where {{ parse_and_filter_future_date('registro_data') }} >= date('{{ last_partition }}')
      {% endif %}
    ),

    estabelecimentos as (
      select 
        id_cnes, 
        nome_acentuado as nome
      from {{ref('dim_estabelecimento')}}
    )

select 
    id_cnes as cnes,
    {{proper_estabelecimento('nome')}} as nome,
    data_registro,
    atendimentos
from atendimento_unidade a
inner join estabelecimentos using(id_cnes)


