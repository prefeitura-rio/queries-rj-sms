{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_pcsm",
        materialized="table" 
    )
}}

with 
   atendimento as (
    select
        id_hci,
        id_atendimento,
        datetime(data_entrada_atendimento, parse_time('%H%M', hora_entrada_atendimento)) as data_hora_entrada,
        datetime(data_saida_atendimento, parse_time('%H%M', hora_saida_atendimento)) as data_hora_saida,
        ta.descricao_classificacao_atendimento as tipo_atendimento,
        ta.descricao_tipo_atendimento as descricao_tipo_atendimento,
        ta.descricao_acao_pos_atendimento as acao_pos_atendimento,
        -- id_profissional_saude, Não há tabela para relacionar o profissional
        id_paciente,
        id_unidade_saude as id_unidade,
        id_atividade_grupo,
        id_encaminhamento,
        id_unidade_saude_encaminhada as id_unidade_encaminhada,
        data_inclusao_cadastro as data_cadastro,
        local_atendimento,
        descricao_local_atendimento,
        atendimento_cancelado,
        descricao_atendimento_cancelado,
        lista_profissionais_atendimento
    from {{ ref('raw_pcsm_atendimentos') }} a
    left join {{ ref('raw_pcsm_tipos_atendimentos') }} ta on ta.id_tipo_atendimento = a.id_tipo_atendimento
   ),

    tipo_atendimento as (
        select 
            id_tipo_atendimento,
            descricao_classificacao_atendimento as classificacao_atendimento,
            descricao_tipo_atendimento as tipo_atendimento,
            descricao_acao_pos_atendimento as acao_pos_atendimento
        from {{ ref('raw_pcsm_tipos_atendimentos') }}
    ),

   paciente as (
        select  
            id_paciente,
            numero_cpf_paciente as cpf, 
            numero_cartao_saude as cns
        from {{ ref('raw_pcsm_pacientes') }}
   ),

    unidade as (
        select
            id_unidade_saude as id_unidade,
            nome_unidade_saude as nome_unidade,
            codigo_nacional_estabelecimento_saude as id_cnes,
        from {{ref('raw_pcsm_unidades_saude')}}
    ),

    -- Atividades em grupo
    atividades_grupo as (
            select 
                id_atividade_grupo as id_atividade,
                initcap(nome_atividade_grupo) as atividade_grupo,
                data_inicio_atividade as data_inicio,
                data_termino_atividade as data_termino,
                id_unidade_saude as id_unidade,
                id_tipo_atividade_grupo as id_tipo, 
                local_atividade_grupo as local_atividade
            from {{ ref('raw_pcsm_atividades_grupo') }} 
    ),

    tipo_atividade as (
        select 
            id_tipo_atividade as id_tipo,
            descricao_tipo_atividade as tipo_atividade
        from {{ ref('raw_pcsm_tipos_atividades') }}
    ),

    atividades_grupo_detalhado as (
        select
        struct(ag.id_atividade,
            ag.atividade_grupo,
            ag.data_inicio,
            ag.data_termino,
            u.nome_unidade as nome_unidade_atividade,
            u.id_cnes as id_cnes_atividade,
            ta.tipo_atividade,
            ag.local_atividade) as atividade_grupo
        from atividades_grupo ag
        left join unidade u on ag.id_unidade = u.id_unidade
        left join tipo_atividade ta on ag.id_tipo = ta.id_tipo
    )


select
    a.id_hci,
    a.id_paciente,
    p.cpf,
    a.tipo_atendimento as tipo,
    a.descricao_tipo_atendimento as subtipo,
    a.data_hora_entrada as entrada_datahora,
    a.data_hora_saida as saida_datahora,
from atendimento a
left join paciente p on p.id_paciente = a.id_paciente
left join unidade u on a.id_unidade = u.id_unidade
left join atividades_grupo_detalhado ag on a.id_atividade_grupo = ag.id


