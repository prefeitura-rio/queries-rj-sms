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
  ),

  atendimento_paciente as (
      select 
        p.id_paciente, 
        p.numero_cpf_paciente as cpf,
        p.numero_cartao_saude as cns,
        a.id_atendimento,
        a.id_atividade_grupo
      from {{ref('raw_pcsm_pacientes')}} p
      inner join atendimentos a 
        on p.id_paciente = a.id_paciente
  )

select
    ap.id_paciente, 
    ap.cpf,
    ap.cns,
    struct (
        ag.id_atividade_grupo as id_evento,
        upper(nome_atividade_grupo) as nome,
        ta.descricao_tipo_atividade as tipo,
        data_inicio_atividade as data_inicio,
        data_termino_atividade as data_termino,
        u.codigo_nacional_estabelecimento_saude as id_cnes,
        u.nome_unidade_saude as unidade_nome
    ) as atividade_grupo
from atendimento_paciente ap 
left join {{ref('raw_pcsm_atividades_grupo')}} ag
  on ap.id_atividade_grupo = ag.id_atividade_grupo
left join {{ref('raw_pcsm_tipos_atividades')}} ta 
  on ag.id_tipo_atividade_grupo = ta.id_tipo_atividade
left join {{ref('raw_pcsm_unidades_saude')}} u
  on ag.id_unidade_saude = u.id_unidade_saude
