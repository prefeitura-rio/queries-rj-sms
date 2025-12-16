{{
    config(
        alias="relacao_hasdm_vitahiscare",
        materialized="table"
    )
}}

with
    source as (
        select
            *
        from {{ source("brutos_informes_vitacare_staging", "relacao_hasdm_vitahiscare") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (
            partition by _source_file, indice
            order by _loaded_at desc
        ) = 1
    ),

    extrair_informacoes as (
        select

            {{ process_null("ap") }} as ap,
            {{ process_null("n_cnes_unidade") }} as n_cnes_unidade,
            {{ process_null("nome_unidade_de_saude") }} as nome_unidade_de_saude,
            {{ process_null("cod_ine_equipe_de_saude") }} as cod_ine_equipe_de_saude,
            {{ process_null("endereco") }} as endereco,
            {{ process_null("cod_area") }} as cod_area,
            {{ process_null("cod_microarea") }} as cod_microarea,
            {{ process_null("tel1") }} as tel1,
            {{ process_null("tel2") }} as tel2,
            {{ process_null("n_do_prontuario") }} as n_do_prontuario,
            {{ process_null("cpf") }} as cpf,
            {{ process_null("dnv") }} as dnv,
            {{ process_null("cns") }} as cns,
            {{ process_null("nis") }} as nis,
            {{ process_null("nome") }} as nome,
            {{ process_null("nome_social") }} as nome_social,
            {{ process_null("nome_mae") }} as nome_mae,
            {{ process_null("sexo") }} as sexo,
            {{ process_null("idade") }} as idade,
            cast(
                {{ process_null("data_nasc") }}
                as date format "DD/MM/YYYY"
            ) as data_nascimento,
            {{ process_null("cor") }} as cor,
            {{ process_null("n_escolar") }} as n_escolar,

            cast(
                {{ process_null("dt_diag_dm") }}
                as date format "DD/MM/YYYY"
            ) as data_diagnostico_dm,
            {{ process_null("cid_ent_dm") }} as cid_ent_dm,

            cast(
                {{ process_null("dt_diag_has") }}
                as date format "DD/MM/YYYY"
            ) as data_diagnostico_has,
            {{ process_null("cid_ent_has") }} as cid_ent_has,

            {{ process_null("tab") }} as tab,
            {{ process_null("tab_tempo") }} as tab_tempo,
            {{ process_null("tab_quant") }} as tab_quant,

            cast(
                {{ process_null("dt_ult_cons_med") }}
                as date format "DD/MM/YYYY"
            ) as data_ultima_consulta_med,
            {{ process_null("cons_med_total") }} as cons_med_total,

            cast(
                {{ process_null("dt_ult_cons_enf") }}
                as date format "DD/MM/YYYY"
            ) as data_ultima_consulta_enf,
            {{ process_null("cons_enf_total") }} as cons_enf_total,

            cast(
                {{ process_null("dt_ult_cons_sb") }}
                as date format "DD/MM/YYYY"
            ) as data_ultima_consulta_sb,
            cast(
                {{ process_null("dt_ult_visita_acs") }}
                as date format "DD/MM/YYYY"
            ) as data_ultima_visita_acs,

            {{ process_null("ativ_fis") }} as ativ_fis,
            {{ process_null("peso") }} as peso,
            {{ process_null("altura") }} as altura,
            {{ process_null("imc") }} as imc,
            {{ process_null("cc_atual") }} as cc_atual,

            {{ process_null("retinografia_esq") }} as retinografia_esq,
            cast(
                {{ process_null("dt_retin_esq") }}
                as date format "DD/MM/YYYY"
            ) as data_retinografia_esq,
            {{ process_null("retinografia_dir") }} as retinografia_dir,
            cast(
                {{ process_null("dt_retin_dir") }}
                as date format "DD/MM/YYYY"
            ) as data_retinografia_dir,

            {{ process_null("fundoscopia_esq") }} as fundoscopia_esq,
            cast(
                {{ process_null("dt_fundoscopia_esq") }}
                as date format "DD/MM/YYYY"
            ) as data_fundoscopia_esq,
            {{ process_null("fundoscopia_dir") }} as fundoscopia_dir,
            cast(
                {{ process_null("dt_fundoscopia_dir") }}
                as date format "DD/MM/YYYY"
            ) as data_fundoscopia_dir,

            cast(
                {{ process_null("dt_pes_aval") }}
                as date format "DD/MM/YYYY"
            ) as data_pes_aval,
            {{ process_null("categ_risc_mmii_esq") }} as categ_risc_mmii_esq,
            {{ process_null("categ_risc_mmii_dir") }} as categ_risc_mmii_dir,

            {{ process_null("amput_mmii_esq") }} as amput_mmii_esq,
            cast(
                {{ process_null("dt_amput_esq") }}
                as date format "DD/MM/YYYY"
            ) as data_amput_esq,
            {{ process_null("amput_mmii_dir") }} as amput_mmii_dir,
            cast(
                {{ process_null("dt_amput_dir") }}
                as date format "DD/MM/YYYY"
            ) as data_amput_dir,

            {{ process_null("colesterol_total") }} as colesterol_total,
            cast(
                {{ process_null("dt_colest_total") }}
                as date format "DD/MM/YYYY"
            ) as data_colesterol_total,

            {{ process_null("ldl") }} as ldl,
            cast(
                {{ process_null("dt_ldl") }}
                as date format "DD/MM/YYYY"
            ) as data_ldl,

            {{ process_null("hdl") }} as hdl,
            cast(
                {{ process_null("dt_hdl") }}
                as date format "DD/MM/YYYY"
            ) as data_hdl,

            {{ process_null("triglicerideos") }} as triglicerideos,
            cast(
                {{ process_null("dt_trig") }}
                as date format "DD/MM/YYYY"
            ) as data_triglicerideos,

            {{ process_null("pas") }} as pas,
            {{ process_null("pas_cat") }} as categ_pas,
            {{ process_null("pad") }} as pad,
            {{ process_null("pad_cat") }} as categ_pad,
            cast(
                {{ process_null("dt_pa") }}
                as date format "DD/MM/YYYY"
            ) as data_pa,

            {{ process_null("hba1c") }} as hba1c,
            {{ process_null("hba1c_cat") }} as categ_hba1c,
            {{ process_null("complicacoes_sec") }} as complicacoes_sec,

            {{ process_null("creatinina") }} as creatinina,
            {{ process_null("dt_creatinina") }}
            cast(
                {{ process_null("dt_creatinina") }}
                as date format "DD/MM/YYYY"
            ) as daat_creatinina,

            {{ process_null("situacao_cad") }} as situacao_cad,

            struct(
                _source_file as arquivo_fonte,
                safe_cast(REGEXP_EXTRACT(_source_file, r'/(\d{4}-\d{2}-\d{2})/') as timestamp) as criado_em,
                safe_cast(_extracted_at as timestamp) as extraido_em,
                safe_cast(_loaded_at as timestamp) as carregado_em
            ) as metadados,

            {{ process_null("ano_particao") }} as ano_particao,
            {{ process_null("mes_particao") }} as mes_particao,
            {{ process_null("data_particao") }} as data_particao

        from sem_duplicatas
    )
select *
from extrair_informacoes
