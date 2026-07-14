{{
    config(
        schema="intermediario_vacinacao",
        alias="vitacare_api", 
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
      id_equipe,
      id_equipe_ine,
      id_microarea,
      paciente_cns,
      paciente_cpf,
      paciente_nome,
      paciente_nascimento_data,
      paciente_nome_mae,
      paciente_obito,
      vacina_descricao,
      vacina_dose,
      vacina_lote,
      vacina_aplicacao_data,
      vacina_registro_data,
      vacina_registro_tipo,
      profissional_nome,
      profissional_cbo,
      profissional_cns,
      profissional_cpf,
      metadados,
      particao_data_vacinacao
    from {{ ref('raw_prontuario_vitacare_api_centralizadora__vacinacao') }}
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
      v.id_equipe_ine as id_ine_equipe,
      v.id_microarea,
      e.nome_limpo as estabelecimento_nome,

      -- vacina
      lower(dv.nome_padronizado) as vacina_nome,
      dv.codigo_sipni as vacina_codigo,
      v.vacina_dose,
      v.vacina_lote,
      v.vacina_aplicacao_data,
      v.vacina_registro_data,
      v.vacina_registro_tipo,

      -- paciente
      v.paciente_cns,
      v.paciente_cpf,
      v.paciente_nome,
      v.paciente_nascimento_data,
      v.paciente_nome_mae,
      case when v.paciente_obito is not null then true else null end as paciente_obito,

      -- profissional
      v.profissional_nome,
      v.profissional_cbo,
      v.profissional_cns,
      v.profissional_cpf,

      -- metadados
      v.metadados.loaded_at as loaded_at,
      v.metadados.updated_at as updated_at,
      v.particao_data_vacinacao as data_particao

    from vacinacao v
    left join estabelecimento e on v.id_cnes = e.id_cnes
    left join depara dv on lower(v.vacina_descricao) = dv.nome_original
  )

select * from final
