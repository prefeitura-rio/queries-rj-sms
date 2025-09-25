{{ config(materialized="table", schema="projeto_c34", alias="producoes_sia") }}

with
    sigtap as (
        select distinct
            procedimento_id,
            upper(trim({{ process_null("procedimento") }})) as procedimento
        from {{ source("sub_geral_prod", "c34_sigtap") }}
    ),

    cns as (
        select cns_id, cns_item
        from {{ref("raw_projeto_c34__cns_fuzzy_match")}}
        left join unnest(cns_array) as cns_item
    ),

    sia as (
        select distinct
            to_hex(
                sha256(cast(safe_cast(paciente_cpf as int64) as string))
            ) as paciente_id,
            safe_cast(paciente_cpf as int64) as paciente_cpf,
            safe_cast(paciente_cns as int64) as paciente_cns,

            "SIA" as sistema,

            procedimento_id,
            parse_date('%Y%m%d', safe_cast(data_realizacao as string)) as data_realizacao,
            unidade_executante_cnes,
            procedimento_qtd,
            parse_date('%Y%m%d', safe_cast(data_inicial_apac as string)) as data_inicial_apac,
            competencia_realizacao_proced,
            left(cid_principal, 3) as cid_execucao_procedimento
        from {{ source("sub_geral_prod", "c34_sia_2024") }}
        where paciente_cns in (select cns_item from cns)
    ),

    sia_cns as (
        select
            cns.cns_id,
            sia.*
        from sia
        inner join cns on paciente_cns = cns_item
    ),

    consolidado as (
        select
            coalesce(obitos.paciente_id, sia_cns.paciente_id) as paciente_id,
            obitos.paciente_cpf_recuperado,
            sistema,
            competencia_realizacao_proced,
            data_realizacao,
            data_inicial_apac,

            procedimento_id,
            procedimento_qtd,
            cid_execucao_procedimento,
            unidade_executante_cnes,

            obitos.paciente_mes_obito,
            obitos.paciente_sexo,
            obitos.paciente_raca_cor,
            obitos.paciente_mun_res_obito_ibge,
            obitos.paciente_bairro_res_obito,
            obitos.paciente_escolaridade_obito,
            obitos.paciente_estado_civil_obito,
            obitos.paciente_faixa_etaria_obito,
            obitos.obito_causabas_cid,
            obitos.obito_mun_ocor_ibge,
            obitos.obito_bairro_ocor,
            obitos.obito_estab_ocor_cnes

        from sia_cns
        inner join {{ ref("raw_projeto_c34__obitos_sim") }} as obitos
        using (cns_id)
    ),

    enriquecimento as (
        select
            c.*,
            sig.procedimento as procedimento,
            
            case
                when
                    unidade_executante_cnes is not null
                    and estab_exec.nome_fantasia is null
                then 'NAO'
                else 'SIM'
            end as unidade_executante_mrj_sus,

            estab_exec.nome_fantasia as unidade_executante,
            estab_exec.esfera_subgeral as unidade_executante_esfera,
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
            estab_obit.tipo_unidade_agrupado_subgeral as obito_estab_ocor_tp,

            bairros_aps_exec.ap as unidade_executante_ap,
            bairros_aps_exec.ap_titulo as unidade_executante_ap_descr,

            bairros_aps_obit.ap as obito_estab_ocor_ap,
            bairros_aps_obit.ap_titulo as obito_estab_ocor_ap_descr,

            upper(cids_exec.categoria_descricao) as cid_execucao_procedimento_descr,
            upper(cids_obit.categoria_descricao) as obito_causabas_cid_descr,

            cids_c34_exec.indicador_cancer as cid_execucao_procedimento_indicador_ca,
            cids_c34_exec.indicador_cancer_pulmao,
            cids_c34_obit.indicador_cancer as obito_causabas_cid_indicador_ca,
            cids_c34_obit.indicador_cancer_pulmao as obito_causabas_cid_indicador_cp,

            upper(ibge_res.nome_municipio) as paciente_mun_res_obito,
            upper(ibge_ocor.nome_municipio) as obito_mun_ocor,

            if(
                c.obito_estab_ocor_cnes is not null, 'SIM', 'NAO'
            ) as obito_estab_ocor_preenchido

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

        left join sigtap as sig
            on c.procedimento_id = sig.procedimento_id
    )

select distinct
    -- [Identificação do registro/paciente e contexto temporal]
    paciente_id,
    paciente_cpf_recuperado,
    sistema,
    competencia_realizacao_proced,
    data_realizacao,
    data_inicial_apac,

    -- [Procedimento executado]
    procedimento_id,
    procedimento,
    procedimento_qtd,

    -- [CID do procedimento executado]
    cid_execucao_procedimento,
    cid_execucao_procedimento_descr,
    cid_execucao_procedimento_indicador_ca,
    indicador_cancer_pulmao,

    -- [Unidade executante do procedimento]
    unidade_executante_cnes,
    unidade_executante_mrj_sus,
    unidade_executante,
    unidade_executante_esfera,
    unidade_executante_tp,
    unidade_executante_ap,
    unidade_executante_ap_descr,

    -- [Informações sobre o óbito - causa básica e indicadores]
    obito_causabas_cid,
    obito_causabas_cid_descr,
    obito_causabas_cid_indicador_ca,
    obito_causabas_cid_indicador_cp,

    -- [Estabelecimento do óbito e classificações]
    obito_estab_ocor_cnes,
    obito_estab_ocor_preenchido,
    obito_estab_ocor_mrj_sus,
    obito_estab_ocor,
    obito_estab_ocor_esfera,
    obito_estab_ocor_tp,
    obito_estab_ocor_ap,
    obito_estab_ocor_ap_descr,

    -- [Localização - residência do paciente e local do óbito]
    paciente_mun_res_obito_ibge,
    paciente_mun_res_obito,
    paciente_bairro_res_obito,
    obito_mun_ocor_ibge,
    obito_mun_ocor,
    obito_bairro_ocor,

    -- [Características demográficas do paciente no óbito]
    paciente_sexo,
    paciente_raca_cor,
    paciente_escolaridade_obito,
    paciente_estado_civil_obito,
    paciente_faixa_etaria_obito,
    paciente_mes_obito

from enriquecimento
