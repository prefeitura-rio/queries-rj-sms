{{
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'atendimento_mandado_filtros'
    )
}}

with base as (

    select *
    from {{ ref('int_cdi__atendimento_mandado') }}

),

base_tratada as (

    select
        data_entrada,
        tipo_de_documento,
        tipo_de_solicitacao,
        pacientes_novos,
        direcionamento_interno,
        sem_deferimento_ou_extinto,
        case when cap is null then 'Sem informação' else cap end as cap,
        sexo,
        estadual_federal,
        advogado_ou_defensoria,
        multas,
        responsavel_pela_saida_juridico,
        prazo_limite,
        data_de_saida,
        orgao_de_origem,

        case
            when sem_deferimento_ou_extinto != 'Sem informação' then 'Sem Deferimento ou Extinto'
            when data_de_saida is not null and prazo_limite is not null and data_de_saida <= prazo_limite then 'No prazo'
            when data_de_saida is not null and prazo_limite is not null and data_de_saida > prazo_limite then 'Fora do prazo'
            when data_de_saida is null and prazo_limite is not null and current_date() <= prazo_limite then 'Dentro do prazo'
            when data_de_saida is null and prazo_limite is not null and current_date() > prazo_limite then 'Vencido'
            else 'Sem prazo'
        end as status_prazo,

        count(processo) as total_demandas

    from base
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

)

select * from base_tratada