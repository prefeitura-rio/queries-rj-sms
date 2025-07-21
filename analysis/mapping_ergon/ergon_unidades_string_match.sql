with

mapping as (
  select distinct
    string_field_1 as nome_mapping,
  from `rj-sms-sandbox.sub_geral_prod.ergon_mapping_unidades`
  where string_field_1 != "PARA"
),

cnes as (
  select distinct
    id_ap as ap,
    nome_fantasia,
    id_cnes
  from `rj-sms.saude_cnes.estabelecimento_sus_rio_historico`
  where nome_fantasia is not null
),

consolidado as (
    select
        mapping.nome_mapping,
        cnes.ap as ap,
        cnes.nome_fantasia,
        cnes.id_cnes

    from mapping
    cross join cnes
),

scores as (
    select
        *,
        {{calculate_lev("nome_mapping", "nome_fantasia")}} as score_lev,
        {{calculate_jaccard("nome_mapping", "nome_fantasia")}} as score_jac
    from consolidado
),

best_match as (
    select
        *,
        row_number() over (partition by nome_mapping order by score_lev asc, score_jac asc) as rn
    from scores
)

select * except (rn)
from best_match
where rn = 1
order by score_lev asc
