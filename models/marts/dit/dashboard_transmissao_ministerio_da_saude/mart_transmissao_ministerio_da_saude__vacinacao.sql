{{ 
    config(
        alias='vacinacao',
        materialized='table',
    ) 
}}
WITH

vacinacoes_api as (
  SELECT 
    v.id_vacinacao, 
    v.vacina_registro_data, 
    v.vacina_aplicacao_data,
    vacina_registro_tipo
  FROM {{ ref("raw_prontuario_vitacare__vacinacao") }} v
    LEFT JOIN {{ ref("dim_estabelecimento") }} e on v.id_cnes = e.id_cnes
  WHERE
    v.vacina_registro_tipo not in ('nao aplicavel', 'nao aplicada') and
    (
      (v.vacina_registro_data between '2025-06-16' and '2025-09-24' and e.area_programatica = '10') or
      (v.vacina_registro_data between '2025-06-16' and '2025-09-24' and e.area_programatica = '21') or
      (v.vacina_registro_data between '2025-06-16' and '2025-09-24' and e.area_programatica = '22') or
      (v.vacina_registro_data between '2025-06-16' and '2025-09-24' and e.area_programatica = '31') or
      (v.vacina_registro_data between '2025-06-16' and '2025-09-24' and e.area_programatica = '32') or
      (v.vacina_registro_data between '2025-06-16' and '2025-09-24' and e.area_programatica = '33')
    )
),
vacinacoes_bkp as (
  SELECT 
    v.id_vacinacao, 
    cast(v.data_registro as date) as vacina_registro_data, 
    v.data_aplicacao as vacina_aplicacao_data,
    lower({{remove_accents_upper("v.tipo_registro")}}) as vacina_registro_tipo
  FROM {{ ref("raw_prontuario_vitacare_historico__vacina") }} v
    LEFT JOIN {{ ref("dim_estabelecimento") }} e on v.id_cnes = e.id_cnes
  WHERE
    v.tipo_registro not in ('Não aplicavel', 'Não aplicada') and
    (
      (v.data_registro between '2025-06-16' and '2025-09-24' and e.area_programatica = '10') or
      (v.data_registro between '2025-06-16' and '2025-09-24' and e.area_programatica = '21') or
      (v.data_registro between '2025-06-16' and '2025-09-24' and e.area_programatica = '22') or
      (v.data_registro between '2025-06-16' and '2025-09-24' and e.area_programatica = '31') or
      (v.data_registro between '2025-06-16' and '2025-09-24' and e.area_programatica = '32') or
      (v.data_registro between '2025-06-16' and '2025-09-24' and e.area_programatica = '33')
    )
),
vacinacoes_merge as (
  select * from vacinacoes_api
  union all
  select * from vacinacoes_bkp
),
vacinacoes as (
  select distinct * from vacinacoes_merge
),

formatted as (
  SELECT 
    coalesce(v.id_vacinacao, id_vacinacao_global) as id_vacinacao, 
    v.vacina_registro_data, 
    v.vacina_aplicacao_data,
    v.vacina_registro_tipo,
    t.status_envio, 
    t.uuid_local, 
    t.uuid_rnds
  FROM vacinacoes v
    FULL OUTER JOIN {{ ref("raw_prontuario_vitacare__transmissao") }} t on id_vacinacao = id_vacinacao_global
  ORDER BY v.vacina_aplicacao_data
),

final as (
  select 
    *,
    case
      when uuid_local is null then 'Não Enviado'
      when uuid_rnds is null then 'Erro no Envio'
      else 'Enviado com Sucesso'
    end as caso
  from formatted
  qualify row_number() over(partition by id_vacinacao order by vacina_aplicacao_data desc) = 1
)
select * 
from final