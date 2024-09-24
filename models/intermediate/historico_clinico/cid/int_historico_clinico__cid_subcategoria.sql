--Cria tabela de de-para da subcategoria e sua descrição resumida. 
--O resumo de descrição é construído como o nível de agrupamento da subcategoria que possui o menor número de caracteres.
with
    cids as (
        select
            id,
            categoria.id as id_categoria,
            cid.descricao as subcategoria_descricao,
            {{clean_cid('cid.descricao')}} as subcategoria_descricao_c,
            categoria.descricao as categoria_descricao,
            {{clean_cid('categoria.descricao')}} as categoria_descricao_c,
            grupo.descricao as grupo_descricao,
            {{clean_cid('grupo.descricao')}} as grupo_descricao_c,
            char_length({{clean_cid('cid.descricao')}}) as len_subcategoria,
            char_length({{clean_cid('categoria.descricao')}}) as len_categoria,
            char_length({{clean_cid('grupo.descricao')}}) as len_grupo,

        from {{ ref("dim_condicao_cid10") }} as cid, unnest(grupo) as grupo

        qualify dense_rank() over (partition by id order by grupo.comprimento asc) = 1
    ),
    -- - 4 DIGITOS ---
    pivoting_4_dig as (
        select *
        from
            cids
            unpivot (len for agrupador in (len_subcategoria, len_categoria, len_grupo))
    ),

    get_min_len_4_dig as (
        select id, min(len) as min_len from pivoting_4_dig group by 1
    ),

    get_best_agg_4_dig as (

        select get_min_len_4_dig.*, pivoting_4_dig.len, pivoting_4_dig.agrupador,

        from get_min_len_4_dig

        left join
            pivoting_4_dig
            on pivoting_4_dig.id = get_min_len_4_dig.id
            and pivoting_4_dig.len = get_min_len_4_dig.min_len

        qualify row_number() over (partition by get_min_len_4_dig.id) = 1
    ),

    agg_4_dig as (
        select
            get_best_agg_4_dig.id,
            subcategoria_descricao,
            categoria_descricao,
            grupo_descricao,
            id_categoria,
            case
                when agrupador = 'len_categoria'
                then categoria_descricao_c
                when agrupador = 'len_subcategoria'
                then subcategoria_descricao_c
                when agrupador = 'len_grupo'
                then grupo_descricao_c
            end as best_agrupador
        from get_best_agg_4_dig
        left join cids on get_best_agg_4_dig.id = cids.id
    )

select 
    id, 
    subcategoria_descricao,
    id_categoria,
    CASE 
        WHEN (char_length(best_agrupador)-1 > 0)
        THEN 
        CONCAT(
            UPPER(LEFT(best_agrupador,1)),
            RIGHT(best_agrupador,char_length(best_agrupador)-1)
        ) 
        ELSE best_agrupador
    END as best_agrupador
from agg_4_dig
where id not in  ('U071', 'U072')
-- Contornando CIDs de COVID que atualmente fazem parte de um grupos de "códigos para uso de emergência", sendo não informativo para o resumo
union all
select 'U071', 'COVID19, virus identificado', 'U07', 'COVID19'
union all
select 'U072', 'COVID19, virus não identificado', 'U07', 'COVID19'