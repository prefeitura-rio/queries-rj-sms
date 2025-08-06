select
    id as cid_id,
    descricao as cid,
    categoria as cid_categoria,
    capitulo as cid_capitulo,
    grupo as cid_grupo
from {{ref("dim_condicao_cid10")}}
