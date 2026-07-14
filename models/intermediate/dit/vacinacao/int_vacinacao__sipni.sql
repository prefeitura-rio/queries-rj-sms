{{
    config(
        schema="intermediario_vacinacao",
        alias="sipni_historico", 
        materialized="table",
        unique_key = ['id_vacinacao'],
        cluster_by= ['id_cnes', 'vacina_nome'],
        tags=['daily', 'vacinacao'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}

with
  vacinacao as (
    select
      id_vacinacao,
      id_cnes,
      paciente_cns,
      paciente_cpf,
      paciente_nome,
      paciente_nascimento_data,
      paciente_nome_mae,
      vacina_nome,
      vacina_dose,
      vacina_aplicacao_data,
      vacina_lote,
      vacina_tipo_registro,
      profissional_nome,
      loaded_at,
      current_datetime('America/Sao_Paulo') as updated_at
    from {{ ref('raw_sipni__vacinacao') }}
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
      cast(null as string) as id_equipe,
      cast(null as string) as id_ine_equipe,
      cast(null as string) as id_microarea,
      e.nome_limpo as estabelecimento_nome,

      -- vacina
      lower(dv.nome_padronizado) as vacina_nome,
      dv.codigo_sipni as vacina_codigo,
      {{ padronizar_dose('v.vacina_dose') }} as vacina_dose,
      v.vacina_lote,
      v.vacina_aplicacao_data,
      v.vacina_aplicacao_data as vacina_registro_data,
      v.vacina_tipo_registro as vacina_registro_tipo,

      -- paciente
      v.paciente_cns,
      v.paciente_cpf,
      v.paciente_nome,
      v.paciente_nascimento_data,
      v.paciente_nome_mae,
      cast(null as boolean) as paciente_obito,

      -- profissional
      v.profissional_nome,
      cast(null as string) as profissional_cbo,
      cast(null as string) as profissional_cns,
      cast(null as string) as profissional_cpf,

      -- metadados
      v.loaded_at,
      v.updated_at,
      v.vacina_aplicacao_data as data_particao

    from vacinacao v
    left join estabelecimento e on v.id_cnes = e.id_cnes
    left join depara dv on v.vacina_nome = dv.nome_original
  )

select * from final
