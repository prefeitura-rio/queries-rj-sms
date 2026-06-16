{{ config(
    schema='intermediario_cegonha',
    alias='alerta_historico',
    materialized='incremental',
    cluster_by=['fim_datahora', 'id_contato'],
    tags=['cegonha_digital_15min']
) }}

with alerta_historico_teste as (

    select *
    from {{ ref('int_subpav_cegonha__alerta_historico_teste') }}

),

final as (

    select
        * except (registro_identificado_na_maternidade)
    from alerta_historico_teste
    where registro_identificado_na_maternidade is true

    {% if is_incremental() %}
      -- Mantém o comportamento da query original: só insere registros
      -- com fim_datahora maior que o maior já carregado na tabela destino.
      and fim_datahora > (
          select coalesce(max(fim_datahora), datetime('1900-01-01'))
          from {{ this }}
      )
    {% endif %}

)

select *
from final