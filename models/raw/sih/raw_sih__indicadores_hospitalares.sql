{{
    config(
        schema="brutos_sih",
        alias="indicadores_hospitalares",
        materialized="table",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sih_staging", "indicadores_hospitalares") }}
    ),
    renamed as (
        select
            uf_zi,
            ano_cmpt,
            mes_cmpt,
            espec,
            cgc_hosp,
            n_aih,
            ident,
            cep,
            munic_res,
            nasc,
            sexo,
            uti_mes_in,
            uti_mes_an,
            uti_mes_al,
            uti_mes_to,
            marca_uti,
            uti_int_in,
            uti_int_an,
            uti_int_al,
            uti_int_to,
            diar_acom,
            qt_diarias,
            proc_solic,
            proc_rea,
            val_sh,
            val_sp,
            val_sadt,
            val_rn,
            val_acomp,
            val_ortp,
            val_sangue,
            val_sadtsr,
            val_transp,
            val_obsang,
            val_ped1ac,
            val_tot,
            val_uti,
            us_tot,
            parse_date('%Y%m%d', dt_inter) as dt_inter,
            parse_date('%Y%m%d', dt_saida) as dt_saida,
            diag_princ,
            diag_secun,
            cobranca,
            natureza,
            nat_jur,
            gestao,
            rubrica,
            ind_vdrl,
            munic_mov,
            cod_idade,
            idade,
            dias_perm,
            morte,
            nacional,
            num_proc,
            car_int,
            tot_pt_sp,
            cpf_aut,
            homonimo,
            num_filhos,
            instru,
            cid_notif,
            contracep1,
            contracep2,
            gestrisco,
            insc_pn,
            seq_aih5,
            cbor,
            cnaer,
            vincprev,
            gestor_cod,
            gestor_tp,
            gestor_cpf,
            gestor_dt,
            cnes,
            cnpj_mant,
            infehosp,
            cid_asso,
            cid_morte,
            complex,
            financ,
            faec_tp,
            regct,
            raca_cor,
            etnia,
            sequencia,
            remessa,
            aud_just,
            sis_just,
            val_sh_fed,
            val_sp_fed,
            val_sh_ges,
            val_sp_ges,
            "" as val_uci,
            marca_uci,
            diagsec1,
            diagsec2,
            diagsec3,
            diagsec4,
            diagsec5,
            diagsec6,
            diagsec7,
            diagsec8,
            diagsec9,
            tpdisec1,
            tpdisec2,
            tpdisec3,
            tpdisec4,
            tpdisec5,
            tpdisec6,
            tpdisec7,
            tpdisec8,
            tpdisec9,
            "" as data_carga,
            ano_particao,
            mes_particao,
            data_particao
        from source
    )

select *
from renamed
