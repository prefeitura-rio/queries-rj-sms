{{
    config(
        schema="saude_dados_mestres",
        alias="profissional",
        materialized="table",
    )
}}
with
    dados_c_cns as (
        select
            base_mestre.nome_profissional,
            base_mestre.cod_profissional_sus,
            base_mestre.cartao_nacional_saude,
            base_mestre.cbo_fam_descricao,
            base_mestre.id_registro_conselho,
            base_mestre.tipo_conselho,
        from
            (
                select
                    *,
                    row_number() over (
                        partition by nome_profissional, cod_profissional_sus
                        order by data_registro desc
                    ) as ordernacao
                from {{ ref("int_profissional__vinculo_cnes_serie_historica") }}
            ) as base_mestre
        where cartao_nacional_saude is not null and ordernacao = 1
    ),

    dados_s_cns as (
        select
            base_mestre.nome_profissional,
            base_mestre.cod_profissional_sus,
            base_mestre.cartao_nacional_saude,
            base_mestre.cbo_fam_descricao,
            base_mestre.id_registro_conselho,
            base_mestre.tipo_conselho,
        from
            (
                select
                    *,
                    row_number() over (
                        partition by nome_profissional, cod_profissional_sus
                        order by data_registro desc
                    ) as ordernacao
                from {{ ref("int_profissional__vinculo_cnes_serie_historica") }}
            ) as base_mestre
        where cartao_nacional_saude is null and ordernacao = 1
    )

select base_cpf.cpf, dados_c_cns.*
from dados_c_cns
left join
    `rj-sms-dev.brutos_plataforma_smsrio.profissional_saude_cpf` as base_cpf
    on cartao_nacional_saude = base_cpf.cns_procurado
union all
(select null as cpf, * from dados_s_cns)
