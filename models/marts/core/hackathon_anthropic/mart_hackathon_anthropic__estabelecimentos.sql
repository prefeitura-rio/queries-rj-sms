{{
    config(
        alias="estabelecimentos"
    )
}}

with 

unidades as (
  select
    id_cnes,
    endereco_latitude,
    endereco_longitude
  from {{ref('dim_estabelecimento')}}
),
unidades_incluidas as (
  select distinct
    original.cnes
  from {{ ref('mart_hackathon_anthropic__elegiveis') }}
)

select
  {{ anonimize('unidades.id_cnes', "'hackathon_anthropic'") }} as unidade_id,
  unidades.endereco_latitude,
  unidades.endereco_longitude
from unidades
where id_cnes in (select cnes from unidades_incluidas)



