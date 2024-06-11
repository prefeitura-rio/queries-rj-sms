{{
    config(
        schema="saude_dados_mestres",
        alias="profissional_saude",
        materialized="table",
    )
}}

with
    profissionais_datasus as (
        select
            id_codigo_sus,
            nome,
            cns,
            row_number() over (
                partition by nome, id_codigo_sus
                order by data_atualizacao_origem desc
            ) as ordenacao
        from {{ ref("raw_cnes_web__dados_profissional_sus") }}
    ),
    alocacao as (
        select
            profissional_codigo_sus,
            array_agg(distinct id_cbo ignore nulls) as id_cbo_lista,
            array_agg(distinct cbo ignore nulls) as cbo_lista,
            array_agg(distinct id_cbo_familia ignore nulls) as id_cbo_familia_lista,
            array_agg(distinct cbo_familia ignore nulls) as cbo_familia_lista,
            array_agg(distinct id_tipo_conselho ignore nulls) as id_tipo_conselho_lista,
            array_agg(distinct id_registro_conselho ignore nulls) as id_registro_conselho_lista,
        from
            {{ ref("int_profissional_saude__vinculo_estabelecimento_serie_historica") }}
        where data_registro = ( select max(data_registro) from {{ ref("int_profissional_saude__vinculo_estabelecimento_serie_historica") }})
        group by 1
    ),
    cpf_profissionais as (select * from {{ ref("raw_pacientes") }})

select
    profissionais_datasus.id_codigo_sus as id_profissional_sus,
    cpf_profissionais.cpf as cpf,
    profissionais_datasus.cns,
    profissionais_datasus.nome,
    alocacao.id_cbo_lista,
    alocacao.cbo_lista as cbo_nome_lista,
    alocacao.id_cbo_familia_lista,
    alocacao.cbo_familia_lista as cbo_familia_nome_lista,
    alocacao.id_registro_conselho_lista,
    alocacao.id_tipo_conselho_lista

from (select * from profissionais_datasus where ordenacao = 1) as profissionais_datasus
inner join alocacao as alocacao 
on profissionais_datasus.id_codigo_sus = alocacao.profissional_codigo_sus
left join cpf_profissionais as cpf_profissionais 
on profissionais_datasus.cns = cpf_profissionais.cns_procurado
