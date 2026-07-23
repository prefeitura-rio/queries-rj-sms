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
        description='Série temporal de atendimentos por data de entrada no prontuário ProntuaRio',
        tags=['datalake']
    )
}}

{% set last_partition = get_last_partition_date(this) %}


-- Utilizei a tabela evolução ao invés de triagem porque há unidades que não possuem passagem pela triagem (Ex. Casa de Parto David Capistrano), 
-- mas possuem registro de evolução. A tabela evolução é mais abrangente e garante que todos os atendimentos sejam contabilizados.
-- Vale confirmar essa abordagem com o time do prontuário

select   
    {{ parse_and_filter_future_date('registro_data') }} as data_registro,
    count(distinct gid_prontuario) as atendimentos
from {{ ref('raw_prontuario_prontuaRio__evolucao') }}
{% if is_incremental() %}
    where {{ parse_and_filter_future_date('registro_data') }} >= date('{{ last_partition }}')
{% endif %}
group by 1

