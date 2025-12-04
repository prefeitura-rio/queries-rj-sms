{{
    config(
        alias="bairro",
        materialized="table",
    )
}}

with points as (
  select
    id_bairro,
    nome_regiao_planejamento,
    nome_regiao_administrativa,
    nome,
    st_centroid(geometry) as centroid
  from {{ ref("raw_datario__bairro") }}
)

select
  id_bairro,
  {{ proper_br("nome_regiao_planejamento") }} as nome_regiao_planejamento,
  {{ proper_br("nome_regiao_administrativa") }} as nome_regiao_administrativa,
  {{ proper_br("nome") }} as nome,
  st_y(centroid) as lat,
  st_x(centroid) as lon
from points
