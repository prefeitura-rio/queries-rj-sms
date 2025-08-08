{{
    config(
        materialized = "table",
        alias = "cid"
    )
}}

select
    id as cid_id,
    descricao as cid,
    categoria as cid_categoria,
    capitulo as cid_capitulo,
    grupo as cid_grupo
from {{source("saude_dados_mestres","condicao_cid10")}}
