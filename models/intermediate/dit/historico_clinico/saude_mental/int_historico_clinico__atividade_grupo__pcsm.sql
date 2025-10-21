{{
    config(
        schema="intermediario_historico_clinico",
        alias="saude_mental_atividade_grupo",
        materialized="table",
    )
}}

with 
  atendimentos as (
      select 
        id_atendimento,
        id_paciente,
        id_atividade_grupo,
      from {{ref('raw_pcsm_atendimentos')}}   
      where id_atividade_grupo is not null

      -- Há a presença de registros duplicados (acolhimentos de pacientes no mesmo dia e mesmo horário com id_acolhimento diferente e sequencial)
      qualify row_number() over(
        partition by 
          id_paciente, 
          id_atendimento, 
          data_entrada_atendimento 
          order by id_atendimento desc) = 1
  ),

  atividade_grupo_enriquecido as (
    select
        a.id_paciente, 
        p.numero_cpf_paciente as cpf,
        p.numero_cartao_saude as cns,
        struct (
            ag.id_atividade_grupo as id_atividade,
            upper(nome_atividade_grupo) as nome,
            ta.descricao_tipo_atividade as tipo,
            data_inicio_atividade as data_inicio,
            data_termino_atividade as data_termino,
            u.codigo_nacional_estabelecimento_saude as id_cnes,
            u.nome_unidade_saude as unidade_nome
        ) as atividade_grupo
    from atendimentos a 
    left join {{ref('raw_pcsm_pacientes')}} p 
      on a.id_paciente = p.id_paciente
    left join {{ref('raw_pcsm_atividades_grupo')}} ag
      on a.id_atividade_grupo = ag.id_atividade_grupo
    left join {{ref('raw_pcsm_tipos_atividades')}} ta 
      on ag.id_tipo_atividade_grupo = ta.id_tipo_atividade
    left join {{ref('raw_pcsm_unidades_saude')}} u
      on ag.id_unidade_saude = u.id_unidade_saude
  )

select distinct * from atividade_grupo_enriquecido