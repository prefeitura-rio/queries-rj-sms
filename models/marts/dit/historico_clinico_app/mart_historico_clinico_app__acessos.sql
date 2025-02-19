{{
    config(
        alias="acessos",
        schema="app_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
  -- -----------------------------------------
  -- Dados de Acesso: Sheets
  -- -----------------------------------------
  usuarios_permitidos_sheets as (
    select
      cpf,
      cnes, 
      nivel_acesso
    from {{ ref('raw_sheets__usuarios_permitidos_hci') }}
  ),

  -- -----------------------------------------
  -- Dados de Acesso: Ergon
  -- -----------------------------------------
  funcionarios_ativos_ergon as (
    select
      distinct cpf
    from {{ ref("raw_ergon_funcionarios") }}, unnest(dados) as funcionario_dado
    where status_ativo = true
  ),
  profissionais_cnes as (
    select
      cpf,
      cns
    from {{ ref("dim_profissional_saude") }}
  ),
  unidades_de_saude as (
    select
      id_cnes as cnes,
      tipo_sms_simplificado as tipo
    from {{ ref("dim_estabelecimento") }}
  ),
  vinculos_profissionais_cnes as (
    select
      profissional_cns as cns,
      id_cnes as cnes,
      cbo_agrupador,
      data_ultima_atualizacao
    from {{ ref("dim_vinculo_profissional_saude_estabelecimento") }}
  ),

  -- -----------------------------------------
  -- Enriquecimento de Dados dos Funcionários
  -- -----------------------------------------
  funcionarios_ativos_enriquecido as (
    select
      cpf,
      dados[0].nome as nome,
      cnes,
      cbo_agrupador,
      tipo,
      data_ultima_atualizacao
    from funcionarios_ativos_ergon
      left join profissionais_cnes using (cpf)
      left join vinculos_profissionais_cnes using (cns)
      left join unidades_de_saude using (cnes)
  ),
  -- -----------------------------------------
  -- Pegando Vinculo mais recente
  -- -----------------------------------------
  funcionarios_ativos_enriquecido_ranked as (
    select
      cpf,
      cnes,
      cbo_agrupador,
      tipo,
      row_number() over (partition by cpf order by data_ultima_atualizacao desc) as rn
    from funcionarios_ativos_enriquecido
  ),
  funcionarios_ativos_enriquecido_mais_recente as (
    select
      cpf,
      cnes,
      cbo_agrupador,
      tipo
    from funcionarios_ativos_enriquecido_ranked
    where rn = 1
  ),
  -- -----------------------------------------
  -- Configurando Nivel de Acesso
  -- -----------------------------------------
  usuarios_permitidos_ergon as (
    select
      cpf,
      cnes,
      cbo_agrupador as tipo_profissional,
      tipo as tipo_unidade,
      CASE
        WHEN (cbo_agrupador = 'MÉDICOS' and tipo in ('UPA','HOSPITAL', 'CER', 'CCO','MATERNIDADE')) 
        THEN 'full_permission'

        WHEN (cbo_agrupador = 'MÉDICOS' and tipo in ('CMS','POLICLINICA','CF','CMR','CSE'))
        THEN 'only_from_same_unit' 

        WHEN (cbo_agrupador = 'ENFERMEIROS')
        THEN 'only_from_same_unit' 

        ELSE null
      END as nivel_acesso
    from funcionarios_ativos_enriquecido_mais_recente
  ),
  
  -- -----------------------------------------
  -- Union
  -- -----------------------------------------
  usuarios_permitidos as (
    select
      safe_cast(cpf as int64) as cpf_particao,
      cpf,
      tipo_profissional,
      tipo_unidade,
      coalesce(usuarios_permitidos_sheets.cnes, usuarios_permitidos_ergon.cnes) as cnes,
      coalesce(usuarios_permitidos_sheets.nivel_acesso, usuarios_permitidos_ergon.nivel_acesso) as nivel_acesso,
      (usuarios_permitidos_sheets.cpf is not null) as eh_permitido,
    from usuarios_permitidos_ergon
      full join usuarios_permitidos_sheets using (cpf)
  )

select distinct *
from usuarios_permitidos

