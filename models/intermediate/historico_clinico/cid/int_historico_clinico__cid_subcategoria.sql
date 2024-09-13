with
    cids as (
        select
            id,
            categoria.id as id_categoria,
            cid.descricao,
            categoria.descricao as categoria_descricao,
            grupo.descricao as grupo_descricao,
            grupo.abreviatura as grupo_abreviatura,
            char_length(cid.descricao) as len_subcategoria,
            char_length(categoria.descricao) as len_categoria,
            char_length(grupo.descricao) as len_grupo,

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
        select id, id_categoria, min(len) as min_len from pivoting_4_dig group by 1, 2
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
            descricao,
            get_best_agg_4_dig.id_categoria,
            case
                when agrupador = 'len_categoria'
                then categoria_descricao
                when agrupador = 'len_subcategoria'
                then descricao
                when agrupador = 'len_grupo'
                then grupo_descricao
            end as best_agrupador
        from get_best_agg_4_dig
        left join cids on get_best_agg_4_dig.id = cids.id
    )

select *
from agg_4_dig
where id not in  ('U071', 'U072')
union all
select 'U071', 'COVID19, virus identificado', 'U07', 'COVID19'
union all
select 'U072', 'COVID19, virus não identificado', 'U07', 'COVID19'
