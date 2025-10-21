{{
    config(
        schema="intermediario_historico_clinico",
        alias="saude_mental_matriciamento",
        materialized="table",
    )
}}

with
    matriciamento_tb as (
        select 
            m.id_matriciamento,
            mp.id_paciente,
            upper(nome_matriciamento) as nome_matriciamento,
            id_unidade_saude as id_unidade,
            datetime(data_inicio_matriciamento, parse_time('%H%M', hora_inicio_matriciamento)) as data_inicio,
            descricao_tipo_matriciamento as tipo,
            descricao_forma_matriciamento as forma,
            descricao_evolucao_matriciamento as evolucao,
            mp.loaded_at,
        from {{ ref('raw_pcsm_matriciamentos')}} m
        join {{ ref('raw_pcsm_matriciamento_pacientes') }} mp  on m.id_matriciamento = mp.id_matriciamento
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
    m.id_paciente,
    p.cpf,
    p.cns,
    struct(
        m.id_matriciamento,
        m.nome_matriciamento,
        m.data_inicio,
        m.tipo,
        m.forma,
        m.evolucao,
        u.nome_unidade,
        u.id_cnes
    ) as matriciamentos,
    m.loaded_at
from matriciamento_tb as m
left join paciente as p on m.id_paciente = p.id_paciente
left join unidade as u on m.id_unidade = u.id_unidade
