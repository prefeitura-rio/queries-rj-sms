{{
    config(
        alias="equipes"
    )
}}

with 

unidades as (
  select
    id_cnes as cnes,
    endereco_latitude,
    endereco_longitude
  from {{ref('dim_estabelecimento')}}
),
unidades_incluidas as (
  select distinct
    original.cnes,
    original.ine
  from {{ ref('mart_hackathon_anthropic__elegiveis') }}
)

select
  {{ anonimize('i.ine', "'hackathon_anthropic'") }} as equipe_id,
  u.endereco_latitude,
  u.endereco_longitude
from unidades u
  inner join unidades_incluidas i using(cnes)



