{{
    config(
        alias="estabelecimentos",
        materialized="table"
    )
}}

with

estabelecimentos as (
  select *
  from {{ref('dim_estabelecimento')}}
  where tipo_sms_agrupado = 'APS' and tipo_sms_simplificado in ('CMS', "CF")
),

final as (
  select area_programatica, id_cnes, nome_fantasia, tipo, endereco_latitude, endereco_longitude
  from estabelecimentos
)

select 
  *,
  -- FILTRO DE PRÉ LANÇAMENTO
  id_cnes in (
    '2295032' -- CMS MARIA CRISTINA ROMA PAUGARTTEN
  ) as sistema_ativo_indicador
from final