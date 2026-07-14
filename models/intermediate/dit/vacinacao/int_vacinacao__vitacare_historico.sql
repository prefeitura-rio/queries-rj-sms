{{
    config(
        schema="intermediario_vacinacao",
        alias="vitacare_historico", 
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

      -- keys
      id_vacinacao,
      id_cnes,
      id_cadastro,
      id_profissional,

      -- Variables
      nome_vacina as vacina_nome,
      cod_vacina as vacina_codigo,
      dose as vacina_dose,
      lote as vacina_lote,

      data_aplicacao as vacina_aplicacao_data,
      data_registro as vacina_registro_data, 

      calendario_vacinal_atualizado as vacina_calendario_atualizado,
      tipo_registro as vacina_registro_tipo,
      estrategia_imunizacao as vacina_estrategia,
      foi_aplicada as vacina_foi_aplicada,
      justificativa as vacina_justificativa,

      -- Metadata
      loaded_at,
      updated_at,
      data_registro as data_particao
    
    from {{ ref('raw_prontuario_vitacare_historico__vacina') }}
  ),

  paciente as (
    select 
        id_global as id_paciente,
        id_cnes,
        ap as area_programatica,
        unidade as unidade_nome,
        nome as paciente_nome,
        nome_mae as paciente_nome_mae,
        cns as paciente_cns,
        cpf as paciente_cpf,
        sexo as paciente_sexo,
        data_nascimento as paciente_nascimento_data,
        obito as paciente_obito,

        codigo_equipe as id_equipe,
        ine_equipe as id_ine_equipe,
        microarea as id_microarea,
        npront as paciente_id_prontuario
        
    from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
    qualify row_number() over( partition by id_local, id_cnes order by greatest(data_cadastro, data_atualizacao_cadastro, updated_at) desc
    ) = 1
  ),

  profissional as (
    select 
      id_global as id_profissional,
      profissional_cns,
      profissional_cpf,
      profissional_cbo,
      profissional_nome
    from {{ ref('raw_prontuario_vitacare_historico__profissional') }}
    qualify row_number() over( partition by id_global order by loaded_at desc) = 1
  ),

  estabelecimento as (
    select
      id_cnes,
      nome_limpo
    from {{ ref('dim_estabelecimento') }}
  ),

  final as (
    select
      -- keys & info
      va.id_vacinacao,
      es.id_cnes,
      pa.id_equipe,
      pa.id_ine_equipe,
      pa.id_microarea,
      es.nome_limpo as estabelecimento_nome,


      -- vacinacao
      lower(dv.nome_padronizado) as vacina_nome,
      dv.codigo_sipni as vacina_codigo,
      va.vacina_dose,
      va.vacina_lote,
      va.vacina_aplicacao_data,
      va.vacina_registro_data,
      va.vacina_registro_tipo,

      -- paciente
      pa.paciente_cns,
      pa.paciente_cpf,
      pa.paciente_nome,
      pa.paciente_nascimento_data,
      pa.paciente_nome_mae,
      pa.paciente_obito,
      
      -- profissional
      pr.profissional_nome,
      pr.profissional_cbo,
      pr.profissional_cns,
      pr.profissional_cpf,

      -- metadados
      va.loaded_at,
      va.updated_at,
      va.vacina_registro_data as data_particao

    
    from vacinacao va
    left join paciente pa
      on va.id_cadastro = pa.id_paciente
    left join estabelecimento es 
      on va.id_cnes = es.id_cnes
    left join profissional pr 
      on va.id_profissional = pr.id_profissional
    left join {{ ref("raw_sheets__depara_vacinas") }} dv 
      on va.vacina_nome = dv.nome_original 
  )

select * from final
    