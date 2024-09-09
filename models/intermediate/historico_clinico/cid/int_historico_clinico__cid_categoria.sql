with cids as (
  select distinct
  id_categoria,
  categoria_descricao,
  g.grupo_descricao,
  g.grupo_descricao_abv,
  char_length(categoria_descricao) as len_categoria,
  char_length(g.grupo_descricao) as len_grupo,
  dense_rank() over (partition by id_subcategoria order by g.grupo_descricao_len asc) as ranking
  from {{ref("raw_datasus__cid10")}}, unnest(grupo) as g
),
-- 3 DIGITOS --
pivoting_3_dig as (
  select *
  from ( 
    select distinct
      id_categoria,
      categoria_descricao,
      grupo_descricao,
      grupo_descricao_abv,
      len_categoria,
      len_grupo,
    from cids where ranking = 1
  ) 
  UNPIVOT(len FOR agrupador IN (len_categoria,len_grupo))
),
get_min_len_3_dig as (
  select  id_categoria, min(len) as min_len
  from pivoting_3_dig
  group by 1
),
get_best_agg_3_dig as (
  select 
    get_min_len_3_dig.*,
    pivoting_3_dig.len, 
    pivoting_3_dig.agrupador, 
    row_number() over(partition by get_min_len_3_dig.id_categoria) as ranking
  from get_min_len_3_dig
  left join pivoting_3_dig
  on pivoting_3_dig.id_categoria = get_min_len_3_dig.id_categoria
   and pivoting_3_dig.len = get_min_len_3_dig.min_len
),
agg_3_dig as (
  select 
  get_best_agg_3_dig.id_categoria,
  CASE
    WHEN agrupador = 'len_categoria' THEN categoria_descricao
    WHEN agrupador = 'len_grupo' THEN grupo_descricao
  END as best_agrupador
  from (select * from get_best_agg_3_dig where ranking=1) as get_best_agg_3_dig
  left join ( select * from cids where ranking = 1) as cids_u
  on get_best_agg_3_dig.id_categoria = cids_u.id_categoria
)
select * from agg_3_dig where id_categoria != 'U07'
union all
select 'U07', 'COVID19'