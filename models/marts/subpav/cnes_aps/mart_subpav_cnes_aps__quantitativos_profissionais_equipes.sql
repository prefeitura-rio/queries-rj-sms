{{
    config(
        materialized = 'table',
        alias        = "quantitativos_profissionais_equipes",
        tags         = ["subpav", "cnes_aps"],
        partition_by = {
            "field": "data_particao",
            "data_type": "date"
        },
        cluster_by   = ["competencia", "cnes", "ine", "cod_cbo"]
    )
}}

with qpe_com_ausentes as (
  select
    data_particao,
    ano_particao,
    mes_particao,

    ap,
    ap_formatada,
    cnes,
    nome_unidade,
    ine,
    nm_referencia,

    tipo_equipe_id,
    classificacao_equipe,

    cod_cbo,
    categoria_profissional_composicao,
    categoria_profissional_vacancia_panorama,

    total_profissionais,
    total_40_horas,
    total_20_horas,
    total_desligados,
    total_dias_desligados,

    data_eqp_incompleta,
    data_eqp_vacancia,

    inconsistente,
    origem_linha_qpe

  from {{ ref('int_subpav_cnes_aps__qpe_com_cbo_ausente') }}
),

qpe_original as (
  select
    data_particao,
    cnes,
    ine,
    cod_cbo,
    categoria_profissional_composicao,

    competencia_mes,

    total_prof_masculino,
    total_prof_feminino,
    total_sem_sexo,
    total_24_horas,
    total_outras_horas,
    total_residentes,
    total_preceptores,
    total_contratados

  from {{ ref('int_subpav_cnes_aps__quantitativos_profissionais_equipes') }}
),

competencias as (
  select
    data_particao,
    competencia,
    competencia_id
  from {{ ref('int_subpav_cnes_aps__competencias_legado') }}
),

enriquecido as (
  select
    q.data_particao,
    c.competencia,
    c.competencia_id,

    q.ap,
    q.ap_formatada,
    lpad(cast(q.cnes as string), 7, '0') as cnes,
    q.nome_unidade,
    lpad(cast(q.ine as string), 10, '0') as ine,
    q.nm_referencia,

    q.tipo_equipe_id,
    q.classificacao_equipe,

    q.cod_cbo,
    q.categoria_profissional_composicao,
    q.categoria_profissional_vacancia_panorama,

    q.total_profissionais,
    o.total_prof_masculino,
    o.total_prof_feminino,
    o.total_sem_sexo,
    q.total_40_horas,
    q.total_20_horas,
    o.total_24_horas,
    o.total_outras_horas,
    o.total_residentes,
    o.total_preceptores,
    o.total_contratados,
    q.total_desligados,

    q.data_eqp_incompleta,
    q.data_eqp_vacancia,

    q.inconsistente,
    q.origem_linha_qpe

  from qpe_com_ausentes q
  left join qpe_original o
    on o.data_particao = q.data_particao
    and lpad(cast(o.cnes as string), 7, '0') = lpad(cast(q.cnes as string), 7, '0')
    and lpad(cast(o.ine as string), 10, '0') = lpad(cast(q.ine as string), 10, '0')
    and o.cod_cbo = q.cod_cbo
  left join competencias c
    on c.data_particao = q.data_particao
)

select
  data_particao,
  competencia,
  competencia_id,

  cnes,
  ine,
  nm_referencia,
  cod_cbo,
  categoria_profissional_composicao,
  categoria_profissional_vacancia_panorama,

  cast(null as int64) as cbo_id,
  cast(null as int64) as categ_prof_id,
  cast(null as int64) as equipe_id,
  cast(null as int64) as unidade_id,

  coalesce(total_profissionais, 0) as total_profissionais,
  coalesce(total_prof_masculino, 0) as total_prof_masculino,
  coalesce(total_prof_feminino, 0) as total_prof_feminino,
  coalesce(total_sem_sexo, 0) as total_sem_sexo,
  coalesce(total_40_horas, 0) as total_40_horas,
  coalesce(total_20_horas, 0) as total_20_horas,
  coalesce(total_24_horas, 0) as total_24_horas,
  coalesce(total_outras_horas, 0) as total_outras_horas,
  coalesce(total_residentes, 0) as total_residentes,
  coalesce(total_preceptores, 0) as total_preceptores,
  coalesce(total_contratados, 0) as total_contratados,
  coalesce(total_desligados, 0) as total_desligados,

  case
    when classificacao_equipe = 'ESF' then 1
    when classificacao_equipe = 'ESB' then 2
    when classificacao_equipe = 'NASF' then 3
    when classificacao_equipe = 'ECR' then 4
    when classificacao_equipe = 'EAP' then 5
    else 0
  end as tp_eqp_por_carga_hor,

  tipo_equipe_id,

  data_eqp_incompleta,
  data_eqp_vacancia,

  origem_linha_qpe,

  current_timestamp() as created_at,
  current_timestamp() as updated_at

from enriquecido
