{{
    config(
        schema="intermediario_vacinacao",
        alias="vitacare_historico", 
        materialized="incremental",
        incremental_strategy="merge",
        unique_key = ['id_vacinacao'],
        cluster_by= ['id_cnes', 'vacina_nome'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

select 
  source_vacina as (
    select 

      -- keys
      id_vacinacao,
      id_cnes,
      id_cadastro,
      id_profissional,

      -- Variables
      nome_vacina as vacina_nome,
      cod_vacina as vacina_nome,
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
      current_datatime('America/Sao_Paulo') as updated_at,
      data_registro as data_particao
    ),

    source_paciente as (
        select 
            id_global as id_paciente,
            id_cnes,
            ap as area_programatica,
            unidade as unidade_nome,
            nome as paciente_nome,
            cns as paciente_cns,
            cpf as paciente_cpf,
            sexo as paciente_sexo,
            data_nascimento as paciente_nascimento_data,
            obito as paciente_obito,

            p.codigo_equipe as id_equipe,
            p.ine_equipe as id_ine_equipe,
            p.microarea as id_microarea,
            p.npront as paciente_id_prontuario,
            p.cns as paciente_cns,
            p.cpf as paciente_cpf,
            
            
        from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
        qualify row_number() over( partition by id_local, id_cnes order by greatest(data_cadastro, data_atualizacao_cadastro, updated_at) desc
        ) = 1
    ),
    