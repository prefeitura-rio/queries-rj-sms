with cids as (
  select 
  id_subcategoria,
  id_categoria,
  subcategoria_descricao,
  categoria_descricao,
  g.grupo_descricao,
  g.grupo_descricao_abv,
  char_length(subcategoria_descricao) as len_subcategoria,
  char_length(categoria_descricao) as len_categoria,
  char_length(g.grupo_descricao) as len_grupo,
  dense_rank() over (partition by id_subcategoria order by g.grupo_descricao_len asc) as ranking
  from {{ref("raw_datasus__cid10")}}, unnest(grupo) as g
),
--- 4 DIGITOS ---
pivoting_4_dig as (
select *
from ( select * from cids where ranking = 1) 
UNPIVOT(len FOR agrupador IN (len_subcategoria,len_categoria,len_grupo))
),
get_min_len_4_dig as (
  select id_subcategoria, id_categoria, min(len) as min_len
  from pivoting_4_dig
  group by 1,2
),
get_best_agg_4_dig as (
  select 
    get_min_len_4_dig.*,
    pivoting_4_dig.len, 
    pivoting_4_dig.agrupador, 
    row_number() over(partition by get_min_len_4_dig.id_subcategoria) as ranking
  from get_min_len_4_dig
  left join pivoting_4_dig
  on pivoting_4_dig.id_subcategoria = get_min_len_4_dig.id_subcategoria
   and pivoting_4_dig.len = get_min_len_4_dig.min_len
),
agg_4_dig as (
  select 
  get_best_agg_4_dig.id_subcategoria,
  subcategoria_descricao,
  get_best_agg_4_dig.id_categoria,
  CASE
    WHEN agrupador = 'len_categoria' THEN categoria_descricao
    WHEN agrupador = 'len_subcategoria' THEN subcategoria_descricao
    WHEN agrupador = 'len_grupo' THEN grupo_descricao
  END as best_agrupador
  from (select * from get_best_agg_4_dig where ranking=1) as get_best_agg_4_dig
  left join ( select * from cids where ranking = 1) as cids_u
  on get_best_agg_4_dig.id_subcategoria = cids_u.id_subcategoria
)
select * from agg_4_dig where id_subcategoria != 'U071' and id_subcategoria != 'U072'
union all
select 'U071','COVID19, virus identificado','U07','COVID19'
union all
select 'U072','COVID19, virus não identificado','U07','COVID19'