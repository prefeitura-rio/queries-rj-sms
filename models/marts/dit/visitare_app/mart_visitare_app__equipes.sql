{{
    config(
        alias="equipes"
    )
}}

with 

unidades as (
  select
    id_cnes as unidade_cnes,

    struct(
      endereco_logradouro as logradouro,
      endereco_numero as numero,
      endereco_complemento as complemento,
      endereco_cep as cep,
      endereco_bairro as bairro,
      endereco_latitude as latitude,
      endereco_longitude as longitude
    ) as endereco
  from {{ref('dim_estabelecimento')}}
),
unidades_incluidas as (
  select distinct
    unidade_cnes,
    equipe_ine
  from {{ ref('mart_visitare_app__elegiveis') }}
)

select
  i.equipe_ine,
  u.endereco
from unidades u
  inner join unidades_incluidas i using(unidade_cnes)



