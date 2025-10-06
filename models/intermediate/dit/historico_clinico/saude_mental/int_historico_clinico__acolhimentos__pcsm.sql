{{
    config(
        schema="intermediario_historico_clinico",
        alias="saude_mental_acolhimento",
        materialized="table",
    )
}}
with 
    acolhimentos as (
        select 
            id_acolhimento,
            datetime(
                data_entrada_acolhimento, 
                parse_time('%H%M', hora_entrada_acolhimento)
                ) as datahora_entrada,
            datetime(
                data_saida_acolhimento, 
                parse_time('%H%M', hora_saida_acolhimento)
                ) as datahora_saida,
            --id_profissional, 
            id_paciente, 
            id_unidade_saude as id_unidade,
            --id_profissional_secundario,
            id_funcionario_cadastramento,
            id_tipo_saida_acolhimento,
            if(leito_ocupado = 'S', true, false) as leito_ocupado,
            descricao_turno_acolhimento,
            descricao_tipo_leito as tipo_leito,
            loaded_at
        from {{ref('raw_pcsm_acolhimento')}}
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
    a.id_paciente,
    p.cpf,
    p.cns,
    struct(
        a.id_acolhimento,
        a.datahora_entrada,
        a.datahora_saida,
        u.nome_unidade,
        u.id_cnes,
        a.leito_ocupado
    ) as acolhimentos
from acolhimentos as a
left join pacientes as p on a.id_paciente = p.id_paciente
left join unidade as u on a.id_unidade = u.id_unidade   