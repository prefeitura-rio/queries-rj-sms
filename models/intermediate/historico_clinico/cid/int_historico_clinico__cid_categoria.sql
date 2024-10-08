--Cria tabela de de-para da categoria e sua descrição resumida. 
--O resumo de descrição é construído como o nível de agrupamento da categoria que possui o menor número de caracteres.
with
    cids as (
        select distinct
            categoria.id as id_categoria,
            categoria.descricao as categoria_descricao,
            {{clean_cid('categoria.descricao')}} as categoria_descricao_c,
            grupo.descricao as grupo_descricao,
            {{clean_cid('grupo.descricao')}} as grupo_descricao_c,
            char_length({{clean_cid('categoria.descricao')}}) as len_categoria,
            char_length({{clean_cid('grupo.descricao')}}) as len_grupo,

        from {{ ref("dim_condicao_cid10") }}, unnest(grupo) as grupo
        qualify dense_rank() over (partition by id order by grupo.comprimento asc) = 1
    ),

    -- 3 DIGITOS --
    pivoting_3_dig as (
        select * from cids unpivot (len for agrupador in (len_categoria, len_grupo))
    ),

    get_min_len_3_dig as (
        select id_categoria, min(len) as min_len from pivoting_3_dig group by 1
    ),

    get_best_agg_3_dig as (
        select get_min_len_3_dig.*, pivoting_3_dig.len, pivoting_3_dig.agrupador,
        from get_min_len_3_dig
        left join
            pivoting_3_dig
            on pivoting_3_dig.id_categoria = get_min_len_3_dig.id_categoria
            and pivoting_3_dig.len = get_min_len_3_dig.min_len
        qualify row_number() over (partition by get_min_len_3_dig.id_categoria) = 1
    ),

    agg_3_dig as (
        select
            get_best_agg_3_dig.id_categoria,
            case
                when agrupador = 'len_categoria'
                then categoria_descricao_c
                when agrupador = 'len_grupo'
                then grupo_descricao_c
            end as best_agrupador
        from get_best_agg_3_dig
        left join
            cids
            on get_best_agg_3_dig.id_categoria = cids.id_categoria
    )

select 
    id_categoria,
    CASE 
        WHEN (char_length(best_agrupador)-1 > 0)
        THEN 
            CONCAT(
                UPPER(LEFT(best_agrupador,1)),
                RIGHT(best_agrupador,char_length(best_agrupador)-1)
            )
        ELSE 
            best_agrupador
    END as best_agrupador
from agg_3_dig where id_categoria != 'U07'
-- Contornando CIDs de COVID que atualmente fazem parte de um grupos de "códigos para uso de emergência", sendo não informativo para o resumo
union all
select 'U07', 'COVID19'