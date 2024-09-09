{{
    config(
        alias="cid10",
        schema = "brutos_datasus",
        labels = {
            'dado_publico': 'sim',
            'dado_pessoal': 'nao',
            'dado_anonimizado': 'nao',
            'dado_sensivel_saude': 'nao',
        }
    )
}}
select 
    safe_cast(id_subcategoria as string) as id_subcategoria,
    safe_cast(id_categoria as string) as id_categoria,
    safe_cast(id_capitulo as string) as id_capitulo,
    safe_cast(ordem as int) as ordem,
    safe_cast(subcategoria_descricao as string) as subcategoria_descricao,
    safe_cast(categoria_descricao as string) as categoria_descricao,
    safe_cast(categoria_descricao_abv as string) as categoria_descricao_abv,
    safe_cast(grupo_descricao as string) as grupo_descricao,
    safe_cast(grupo_descricao_abv as string) as grupo_descricao_abv,
    safe_cast(grupo_descricao_len as int) as grupo_descricao_len,
    safe_cast(capitulo_descricao as string) as capitulo_descricao 
from 
{{ source("brutos_datasus_staging", "cid10") }}