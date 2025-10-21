{{
    config(
        schema="intermediario_historico_clinico",
        alias="saude_mental_articulacao",
        materialized="table",
    )
}}

with 
    articulacao as (
        select 
            a.id_articulacao,
            upper(nome_articulacao) as nome,
            id_paciente,
            datetime(data_entrada_articulacao, parse_time('%H%M', hora_entrada_articulacao)) as datahora_inicio,
            cast(null as datetime) as datahora_termino, -- Não existe este campo na tabela
            descricao_tipo_articulacao as tipo,
            descricao_forma_articulacao as forma,
            descricao_evolucao_articulacao as evolucao,
            id_unidade_saude as id_unidade,
            descricao_paciente_evoluido as paciente_evoluido,
            a.loaded_at
        from {{ref('raw_pcsm_articulacoes')}} a
        left join {{ref('raw_pcsm_articulacao_pacientes')}} ap
            on a.id_articulacao = ap.id_articulacao

        -- Há a presença de registros duplicados (acolhimentos de pacientes no mesmo dia e mesmo horário com id_acolhimento diferente e sequencial)
        qualify row_number() over (partition by id_paciente, data_entrada_articulacao order by a.id_articulacao desc) = 1
    ),


    paciente as (
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
    p.cpf,
    p.cns,
    struct(
        a.id_articulacao,
        a.nome,
        a.datahora_inicio,
        a.datahora_termino,
        a.tipo,
        a.forma,
        a.evolucao,
        u.nome_unidade,
        u.id_cnes
    ) as articulacoes,
    a.loaded_at
from articulacao as a
left join paciente as p on a.id_paciente = p.id_paciente
left join unidade as u on a.id_unidade = u.id_unidade

