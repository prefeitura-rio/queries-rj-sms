{{
    config(
        schema="saude_dados_mestres",
        alias="profissional_saude",
        materialized="table",
    )
}}
with
    profissionais as (
        select
            base_mestre.profissional_nome,
            base_mestre.profissional_codigo_sus,
            base_mestre.profissional_cns,
            base_mestre.cbo_familia,
            base_mestre.id_registro_conselho,
            base_mestre.id_tipo_conselho,
        from
            (
                select
                    *,
                    row_number() over (
                        partition by profissional_nome, profissional_codigo_sus
                        order by data_registro desc
                    ) as ordernacao
                from
                    {{
                        ref(
                            "int_profissional_saude__vinculo_estabelecimento_serie_historica"
                        )
                    }}
            ) as base_mestre
        where ordernacao = 1
    ),
    cpf_profissionais as (select * from {{ ref("raw_pacientes") }})

select
    base_cpf.cpf as cpf,
    profissionais.profissional_codigo_sus as codigo_sus,
    profissionais.profissional_nome as nome,
    profissionais.profissional_cns as cns,
    profissionais.cbo_familia as cbo_familia,
    profissionais.id_registro_conselho,
    profissionais.id_tipo_conselho

from profissionais
left join cpf_profissionais as base_cpf on profissional_cns = base_cpf.cns_procurado
