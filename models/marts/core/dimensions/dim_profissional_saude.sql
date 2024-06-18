{{
    config(
        schema="saude_dados_mestres",
        alias="profissional_saude",
        materialized="table",
        tags=["weekly"]
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
    estabelecimentos as (select distinct id_cnes from {{ ref("dim_estabelecimento") }}),
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
            {{ ref("int_profissional_saude__vinculo_estabelecimento_serie_historica") }} as v
        inner join estabelecimentos as estabelecimentos
        on estabelecimentos.id_cnes  = v.id_cnes
        where data_registro = ( select max(data_registro) from {{ ref("int_profissional_saude__vinculo_estabelecimento_serie_historica") }})
        group by 1
    ),
    cpf_profissionais as (
        select  patient_cns.cns_valor as cns, patient.cpf as cpf
        from {{ ref("raw_hci__paciente") }} as patient
        left join {{ ref("raw_hci__cns_paciente")}} as patient_cns
        on patient.id_paciente = patient_cns.id_paciente
    )

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
    alocacao.id_tipo_conselho_lista,
    current_date('America/Sao_Paulo') as data_referencia

from (select * from profissionais_datasus where ordenacao = 1) as profissionais_datasus
inner join alocacao as alocacao 
on profissionais_datasus.id_codigo_sus = alocacao.profissional_codigo_sus
left join cpf_profissionais as cpf_profissionais 
on profissionais_datasus.cns = cpf_profissionais.cns
