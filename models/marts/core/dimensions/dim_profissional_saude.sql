{{
    config(
        schema="saude_dados_mestres",
        alias="profissional_saude",
        materialized="table",
        tags=["weekly"],
    )
}}

with
    estabelecimentos as (select distinct id_cnes from {{ ref("dim_estabelecimento") }}),
    alocacao as (
        select v.*
        from
            {{ ref("int_profissional_saude__vinculo_estabelecimento_serie_historica") }}
            as v
        inner join estabelecimentos on estabelecimentos.id_cnes = v.id_cnes
        where
            data_registro = (
                select max(data_registro)
                from
                    {{
                        ref(
                            "int_profissional_saude__vinculo_estabelecimento_serie_historica"
                        )
                    }}
            )
    ),
    unique_profissionais_datasus as (
        select
            id_codigo_sus,
            data_carga,
            row_number() over (
                partition by id_codigo_sus order by data_carga desc
            ) as ordenacao
        from {{ ref("raw_cnes_web__dados_profissional_sus") }} as unique_p
        inner join alocacao as alocacao
        on unique_p.id_codigo_sus = alocacao.profissional_codigo_sus
    ),
    profissionais_datasus as (
        select
            profissionais_unico.id_codigo_sus,
            profissionais_enriquecido.nome,
            profissionais_enriquecido.cns,
            profissionais_unico.data_carga
        from
            (
                select * from unique_profissionais_datasus where ordenacao = 1
            ) as profissionais_unico
        left join
            {{ ref("raw_cnes_web__dados_profissional_sus") }}
            as profissionais_enriquecido
            on concat(
                profissionais_unico.id_codigo_sus,
                '.',
                profissionais_unico.data_carga
            ) = concat(
                profissionais_enriquecido.id_codigo_sus,
                '.',
                profissionais_enriquecido.data_carga
            )
    ),
    cbo_distinct as (
        select distinct profissional_codigo_sus, id_cbo, cbo, id_cbo_familia, cbo_familia
        from alocacao
    ),
    cbo_agg as (
        select
        profissional_codigo_sus,
        array_agg(struct(id_cbo, cbo, id_cbo_familia, cbo_familia)) as cbo
        from cbo_distinct
        group by 1
    ),
    conselho_distinct as (
        select distinct profissional_codigo_sus, id_tipo_conselho, id_registro_conselho
        from alocacao
    ),
    conselho_agg as (
        select
    profissional_codigo_sus,
    array_agg(struct(id_registro_conselho, id_tipo_conselho)) as conselho
    from conselho_distinct
    group by 1
    ),
    cpf_profissionais as (
        select patient_cns.cns_valor as cns, patient.cpf as cpf
        from {{ ref("raw_hci__paciente") }} as patient
        left join
            {{ ref("raw_hci__cns_paciente") }} as patient_cns
            on patient.id_paciente = patient_cns.id_paciente
    )
select
    profissionais_datasus.id_codigo_sus as id_profissional_sus,
    cpf_profissionais.cpf as cpf,
    profissionais_datasus.cns,
    profissionais_datasus.nome,
    cbo_agg.cbo,
    conselho_agg.conselho,
    current_date('America/Sao_Paulo') as data_referencia
from profissionais_datasus
left join
    cpf_profissionais 
    on profissionais_datasus.cns = cpf_profissionais.cns
left join
    cbo_agg 
    on profissionais_datasus.id_codigo_sus = cbo_agg.profissional_codigo_sus
left join
    conselho_agg 
    on profissionais_datasus.id_codigo_sus = conselho_agg.profissional_codigo_sus
