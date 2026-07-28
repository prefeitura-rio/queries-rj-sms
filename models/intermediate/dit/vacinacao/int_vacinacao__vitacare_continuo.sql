{{
    config(
        schema="intermediario_vacinacao",
        alias="vitacare_continuo", 
        materialized="incremental",
        incremental_strategy="merge",
        unique_key = ['id_vacinacao'],
        cluster_by= ['id_cnes', 'vacina_nome'],
        tags=['daily', 'vacinacao'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with
  vacinacao as (
    select
      id_vacinacao,
      id_prontuario_global,
      id_cnes,
      nome_vacina,
      cod_vacina,
      dose,
      lote,
      data_aplicacao,
      data_registro,
      tipo_registro,
      id_equipe,
      id_ine_equipe,
      profissional_nome,
      profissional_cbo,
      profissional_cns,
      profissional_cpf,
      loaded_at,
      updated_at,
      data_particao
    from {{ ref('raw_prontuario_vitacare_api__vacina') }}
  ),

  cadastro as (
    select
      concat(id_cnes, '.', ut_id) as id_paciente,
      id_cnes,
      cns as paciente_cns,
      cpf as paciente_cpf,
      nome as paciente_nome,
      data_nascimento as paciente_nascimento_data,
      nome_mae as paciente_nome_mae,
      obito as paciente_obito,
      microarea as id_microarea
    from {{ ref('raw_prontuario_vitacare_api__cadastro') }}
  ),

  estabelecimento as (
    select
      id_cnes,
      nome_limpo
    from {{ ref('dim_estabelecimento') }}
  ),

  depara as (
    select
      nome_original,
      nome_padronizado,
      codigo_sipni
    from {{ ref('raw_sheets__depara_vacinas') }}
  ),

  final as (
    select
      -- keys
      v.id_vacinacao,
      v.id_cnes,
      v.id_equipe,
      v.id_ine_equipe,
      ca.id_microarea,
      e.nome_limpo as estabelecimento_nome,

      -- vacina
      lower(dv.nome_padronizado) as vacina_nome,
      dv.codigo_sipni as vacina_codigo,
      {{ padronizar_dose('v.dose') }} as vacina_dose,
      v.lote as vacina_lote,
      v.data_aplicacao as vacina_aplicacao_data,
      cast(v.data_registro as date) as vacina_registro_data,
      v.tipo_registro as vacina_registro_tipo,

      -- paciente
      ca.paciente_cns,
      ca.paciente_cpf,
      ca.paciente_nome,
      ca.paciente_nascimento_data,
      ca.paciente_nome_mae,
      ca.paciente_obito,

      -- profissional
      v.profissional_nome,
      v.profissional_cbo,
      v.profissional_cns,
      v.profissional_cpf,

      -- metadados
      v.loaded_at,
      v.updated_at,
      v.data_particao

    from vacinacao v
    left join cadastro ca on v.id_prontuario_global = ca.id_paciente
    left join estabelecimento e on v.id_cnes = e.id_cnes
    left join depara dv on v.nome_vacina = dv.nome_original
  )

select * from final
