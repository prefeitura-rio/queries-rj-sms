{{
  config(
    schema="intermediario_historico_clinico",
    alias="acessos_automatico_debug",
    materialized="table",
    partition_by={
      "field": "cpf_particao",
      "data_type": "int64",
      "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    }
  )
}}


with
  profissionais_cnes as (
    select
      id_profissional_sus,
      nome,
      cns,
      cpf
    from {{ ref("int_gdb_cnes__profissional") }}
  ),
  vinculos_cnes as (
    select *
    from {{ ref("int_gdb_cnes__vinculo") }}
  ),

  unidades_de_saude as (
    select
      id_cnes,
      id_unidade,
      area_programatica,
      tipo_sms_simplificado,
      nome_limpo as unidade_nome
    from {{ ref("dim_estabelecimento") }}
  ),

  cbo_datasus as (
    select *
    from {{ ref("raw_datasus__cbo") }}
  ),

  vinculos_preenchidos as (
    select
      id_profissional_sus,
      id_cnes,
      unidades_de_saude.unidade_nome,
      cbo_datasus.descricao
    from vinculos_cnes
    left join cbo_datasus
      using (id_cbo)
    left join unidades_de_saude
      using (id_unidade)
  ),
  cnes_ativos as (
    select distinct
      p.cpf,
      p.cpf is not null as cnes_ativo,
      cast(null as bool) as ergon_ativo,

      v.id_cnes as cnes_unidade_id,
      v.unidade_nome as cnes_unidade_nome,
      v.descricao as cnes_cargo,

      cast(null as string) as ergon_setor,
      cast(null as string) as ergon_cargo
    from profissionais_cnes as p
    left join vinculos_preenchidos as v
      using (id_profissional_sus)
  ),
  ergon_ativos as (
    select
      cpf,
      cast(null as bool) as cnes_ativo,
      logical_or(vinculo.status_ativo) as ergon_ativo,

      cast(null as string) as cnes_unidade_id,
      cast(null as string) as cnes_unidade_nome,
      cast(null as string) as cnes_cargo,

      string_agg(vinculo.lotacao.setor_sigla, "; ") as ergon_setor,
      string_agg(vinculo.cargo.cargo_nome, "; ") as ergon_cargo
    from {{ ref("raw_ergon__funcionarios_sms")}},
      unnest(vinculos) as vinculo
    group by 1
  ),
  final as (
    select
      cpf,
      any_value(cnes_ativo) as cnes_ativo,
      any_value(ergon_ativo) as ergon_ativo,
      any_value(cnes_unidade_id) as cnes_unidade_id,
      any_value(cnes_unidade_nome) as cnes_unidade_nome,
      any_value(cnes_cargo) as cnes_cargo,
      any_value(ergon_setor) as ergon_setor,
      any_value(ergon_cargo) as ergon_cargo
    from (
      select * from cnes_ativos
      union all
      select * from ergon_ativos
    )
    where cpf is not null
    group by cpf
  )

select
  cpf,
  ifnull(cnes_ativo, false) as cnes_ativo,
  ifnull(ergon_ativo, false) as ergon_ativo,
  cnes_unidade_id,
  cnes_unidade_nome,
  cnes_cargo,
  ergon_setor,
  ergon_cargo,
  safe_cast(cpf as int64) as cpf_particao
from final
