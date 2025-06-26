{{ config(materialized="table", schema="projeto_c34", alias="marcacoes_sih") }}

with
    execucoes_sih as (
        select
            safe_cast(id_cnes as int64) as unidade_executante_cnes,

            to_hex(
                sha256(cast(safe_cast(paciente_cpf as int64) as string))
            ) as paciente_id,

            "SIH" as sistema,

            date(data_internacao) as data_internacao,
            date(data_saida) as data_saida,
            safe_cast(procedimento_realizado as int64) as procedimento_id,

            case
                when safe_cast(internacao_carater as int64) = 1
                then "ELETIVO"
                when safe_cast(internacao_carater as int64) = 2
                then "URGENCIA"
                else null
            end as internacao_carater,

            case
                when safe_cast(leito_especialidade as int64) = 1
                then "CIRURGICO"
                when safe_cast(leito_especialidade as int64) = 2
                then "OBSTETRICO"
                when safe_cast(leito_especialidade as int64) = 3
                then "CLINICO"
                when safe_cast(leito_especialidade as int64) = 4
                then "CRONICO"
                when safe_cast(leito_especialidade as int64) = 9
                then "LEITO DIA / CIRURGICO"
                else null
            end as leito_especialidade,

            diarias,
            diarias_uti,
            diarias_ui,

            case
                when safe_cast(procedimento_complexidade as int64) = 0
                then "NAO SE APLICA"
                when safe_cast(procedimento_complexidade as int64) = 1
                then "ATENCAO BASICA"
                when safe_cast(procedimento_complexidade as int64) = 2
                then "MEDIA COMPLEXIDADE"
                when safe_cast(procedimento_complexidade as int64) = 3
                then "ALTA COMPLEXIDADE"
                else null
            end as procedimento_complexidade,

            lpad(diagnostico_principal, 3) as cid_execucao_procedimento

        from {{ ref("raw_sih__autorizacoes_internacoes_hospitalares") }}
        where
            safe_cast(paciente_cpf as int64) in (
                select cpf_candidato from {{ ref("raw_projeto_c34__cpfs_fuzzy_match") }}
            )
            and upper(trim(aih_situacao)) = "APROVADA"
    ),

    sigtap as (
        select distinct
            procedimento_id,
            upper(trim({{ process_null("procedimento") }})) as procedimento
        from {{ source("sub_geral_prod", "c34_sigtap") }}
    ),

    enriquece_procedimento as (
        select distinct sih.*, sigtap.procedimento
        from execucoes_sih as sih
        left join sigtap using (procedimento_id)
    ),

    consolidado as (
        select *
        from enriquece_procedimento
        left join {{ ref("raw_projeto_c34__obitos_sim") }} using (paciente_id)
    ),

    enriquecimento as (

        select
            c.*,

            case
                when
                    unidade_executante_cnes is not null
                    and estab_exec.nome_fantasia is null
                then 'NAO'
                else 'SIM'
            end as unidade_executante_mrj_sus,

            estab_exec.nome_fantasia as unidade_executante,
            estab_exec.esfera_subgeral as unidade_executante_esfera,
            bairros_aps_exec.ap as unidade_executante_ap,
            bairros_aps_exec.ap_titulo as unidade_executante_ap_descr,
            estab_exec.tipo_unidade_agrupado_subgeral as unidade_executante_tp,

            case
                when
                    obito_estab_ocor_cnes is not null
                    and estab_obit.nome_fantasia is null
                then 'NAO'
                else 'SIM'
            end as obito_estab_ocor_mrj_sus,

            estab_obit.nome_fantasia as obito_estab_ocor,
            estab_obit.esfera_subgeral as obito_estab_ocor_esfera,
            upper(ibge_ocor.nome_municipio) as obito_mun_ocor,
            bairros_aps_obit.ap as obito_estab_ocor_ap,
            bairros_aps_obit.ap_titulo as obito_estab_ocor_ap_descr,
            estab_obit.tipo_unidade_agrupado_subgeral as obito_estab_ocor_tp,
            if(
                c.obito_estab_ocor_cnes is not null, 'SIM', 'NAO'
            ) as obito_estab_ocor_preenchido,

            cids_c34_exec.indicador_cancer as cid_execucao_procedimento_indicador_ca,
            cids_c34_exec.indicador_cancer_pulmao
            as cid_execucao_procedimento_indicador_cp,
            upper(cids_exec.categoria_descricao) as cid_execucao_procedimento_descr,

            cids_c34_obit.indicador_cancer as obito_causabas_cid_indicador_ca,
            cids_c34_obit.indicador_cancer_pulmao as obito_causabas_cid_indicador_cp,
            upper(cids_obit.categoria_descricao) as obito_causabas_cid_descr,

            upper(ibge_res.nome_municipio) as paciente_mun_res_obito

        from consolidado as c

        -- obtendo dados de estabelecimentos
        left join
            {{ ref("raw_sheets__estabelecimento_auxiliar") }} as estab_exec
            on safe_cast(c.unidade_executante_cnes as int64)
            = safe_cast(estab_exec.id_cnes as int64)
        left join
            {{ ref("raw_sheets__estabelecimento_auxiliar") }} as estab_obit
            on safe_cast(c.obito_estab_ocor_cnes as int64)
            = safe_cast(estab_obit.id_cnes as int64)

        -- obtendo dados de ap
        left join
            {{ ref("dim_estabelecimento_bairro_ap") }} as bairros_aps_exec
            on safe_cast(c.unidade_executante_cnes as int64)
            = safe_cast(bairros_aps_exec.id_cnes as int64)
        left join
            {{ ref("dim_estabelecimento_bairro_ap") }} as bairros_aps_obit
            on safe_cast(c.obito_estab_ocor_cnes as int64)
            = safe_cast(bairros_aps_obit.id_cnes as int64)

        -- obtendo dados de cids
        left join
            {{ ref("raw_datasus__cid10") }} as cids_exec
            on c.cid_execucao_procedimento = cids_exec.id_categoria

        left join
            {{ ref("raw_datasus__cid10") }} as cids_obit
            on c.obito_causabas_cid = cids_obit.id_categoria

        -- obtendo nomes dos municipios
        left join
            {{ ref("raw_sheets__municipios_rio") }} as ibge_res
            on safe_cast(c.paciente_mun_res_obito_ibge as int64)
            = safe_cast(ibge_res.cod_ibge_6 as int64)

        left join
            {{ ref("raw_sheets__municipios_rio") }} as ibge_ocor
            on safe_cast(c.obito_mun_ocor_ibge as int64)
            = safe_cast(ibge_ocor.cod_ibge_6 as int64)

        -- classificações de cids c34
        left join
            {{ ref("raw_sheets__projeto_c34_cids") }} as cids_c34_exec
            on c.cid_execucao_procedimento = cids_c34_exec.cid

        left join
            {{ ref("raw_sheets__projeto_c34_cids") }} as cids_c34_obit
            on c.obito_causabas_cid = cids_c34_obit.cid
    )

select distinct

    /* 1. Identificação geral */
    paciente_id,
    sistema,

    /* 2. Datas da internação */
    data_internacao,
    data_saida,

    /* 3. Características da internação */
    internacao_carater,
    leito_especialidade,
    diarias,
    diarias_uti,
    diarias_ui,

    /* 4. Procedimento / CID executado */
    procedimento_id,
    procedimento,
    procedimento_complexidade,
    cid_execucao_procedimento,
    cid_execucao_procedimento_descr,
    cid_execucao_procedimento_indicador_ca,
    cid_execucao_procedimento_indicador_cp,

    /* 5. Unidade executante (do mais geral ao específico) */
    unidade_executante,
    unidade_executante_esfera,
    unidade_executante_tp,
    unidade_executante_ap,
    unidade_executante_ap_descr,
    unidade_executante_cnes,
    unidade_executante_mrj_sus,

    /* 6. Dados demográficos do paciente */
    paciente_cpf_recuperado,
    paciente_sexo,
    paciente_raca_cor,
    paciente_mun_res_obito_ibge,
    paciente_mun_res_obito,
    paciente_bairro_res_obito,

    /* 7. Informações de óbito (paciente) */
    paciente_mes_obito,
    paciente_faixa_etaria_obito,
    paciente_escolaridade_obito,
    paciente_estado_civil_obito,

    /* 8. Causa básica do óbito */
    obito_causabas_cid,
    obito_causabas_cid_descr,
    obito_causabas_cid_indicador_ca,
    obito_causabas_cid_indicador_cp,

    /* 9. Local do óbito – município/bairro */
    obito_mun_ocor_ibge,
    obito_mun_ocor,
    obito_bairro_ocor,

    /* 10. Estabelecimento do óbito (do geral ao específico) */
    obito_estab_ocor,
    obito_estab_ocor_esfera,
    obito_estab_ocor_tp,
    obito_estab_ocor_ap,
    obito_estab_ocor_ap_descr,
    obito_estab_ocor_cnes,
    obito_estab_ocor_mrj_sus,
    obito_estab_ocor_preenchido

from enriquecimento
