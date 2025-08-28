{{ config(materialized="table", schema="projeto_c34", alias="obitos_sim") }}

with
    obitos_c34_2024_mrj as (
        select
            -- pk
            data_nasc as paciente_data_nasc,
            {{ clean_name_string("upper(nome)") }} as paciente_nome,
            {{ clean_name_string("upper(nome_mae)") }} as paciente_nome_mae,

            -- atts imutaveis
            extract(month from data_obito) as paciente_mes_obito,
            sexo as paciente_sexo,
            raca_cor as paciente_raca_cor,

            -- atts residencia
            res_mun_cod as paciente_mun_res_obito_ibge,
            res_bairro as paciente_bairro_res_obito,

            -- atts socioeconomicos
            escolaridade as paciente_escolaridade_obito,
            estado_civil as paciente_estado_civil_obito,
            case
                when idade between 0 and 15
                then '0-15'
                when idade between 16 and 30
                then '16-30'
                when idade between 31 and 45
                then '31-45'
                when idade between 46 and 60
                then '46-60'
                when idade between 61 and 75
                then '61-75'
                when idade > 75
                then '76+'
                else 'SEM INFORMACAO'
            end as paciente_faixa_etaria_obito,

            -- atts obito
            upper(left(causa_bas, 3)) as obito_causabas_cid,
            ocor_mun_cod as obito_mun_ocor_ibge,
            ocor_bairro as obito_bairro_ocor,
            ocor_estab as obito_estab_ocor_cnes,
            declaracao_obito_num as declaracao_obito_sim

        from {{ source("sub_geral_prod", "c34_obitos_mrj") }} as sim
    ),

    obitos_desidentificados as (
        select
            if(fuzzy.id_paciente is not null, "SIM", "NAO") as paciente_cpf_recuperado,
            fuzzy.id_paciente as paciente_id,
            sim.* except (paciente_nome, paciente_nome_mae, paciente_data_nasc)

        from obitos_c34_2024_mrj as sim
        left join
            {{ ref("raw_projeto_c34__cpfs_fuzzy_match") }} as fuzzy
            on sim.paciente_nome = fuzzy.nome
            and sim.paciente_nome_mae = fuzzy.nome_mae
            and sim.paciente_data_nasc = fuzzy.data_nasc
    ),

    adicao_cns as (
        select 
            ob.* except(declaracao_obito_sim),
            cns_fuzzy.cns_id
        from obitos_desidentificados as ob
        left join {{ref("raw_projeto_c34__cns_fuzzy_match")}} as cns_fuzzy
        using (declaracao_obito_sim)
    )

select distinct *
from adicao_cns
