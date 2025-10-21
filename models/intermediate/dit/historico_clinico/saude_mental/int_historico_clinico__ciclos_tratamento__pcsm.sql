{{
    config(
        schema="intermediario_historico_clinico",
        alias="saude_mental_ciclos_tratamento",
        materialized="table",
    )
}}

with 
    ciclos as (
        select
            id_ciclo,
            id_paciente,
            id_unidade_saude as id_unidade,
            descricao_tipo_ciclo as tipo_ciclo,
            data_inicio_ciclo as data_inicio,
            data_termino_ciclo as data_termino,
            descricao_situacao_paciente_ciclo as situacao,
            loaded_at as data_carga
        from {{ref('raw_pcsm_ciclos_tratamento_pacientes')}}
    ),

    pacientes as (
        select 
            id_paciente,
            numero_cpf_paciente as cpf, 
            numero_cartao_saude as cns,
        from {{ref('raw_pcsm_pacientes')}}
    ),

    unidade as (
        select
            id_unidade_saude as id_unidade,
            nome_unidade_saude as nome_unidade,
            codigo_nacional_estabelecimento_saude as id_cnes,
        from {{ref('raw_pcsm_unidades_saude')}}
    )

select 
    c.id_paciente,
    p.cpf,
    p.cns,
    struct(
        c.id_ciclo as id_ciclo,
        c.tipo_ciclo as tipo,
        c.situacao,
        c.data_inicio as data_inicio,
        c.data_termino as data_termino,
        {{ proper_estabelecimento('nome_unidade') }} as unidade_nome,
        u.id_cnes
    ) as ciclos_tratamento
from ciclos as c
left join pacientes as p on c.id_paciente = p.id_paciente
left join unidade as u on c.id_unidade = u.id_unidade
