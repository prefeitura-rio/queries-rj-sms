{{ config(materialized="table", schema="projeto_c34", alias="marcacoes_ser") }}

with
    source as (
        select
            -- id
            to_hex(
                sha256(cast(safe_cast(paciente_cpf as int64) as string))
            ) as paciente_id,

            "SER" as sistema,

            -- procedimento executado
            upper(trim({{ process_null("procedimento") }})) as procedimento,
            upper(
                trim({{ process_null("procedimento_especialidade") }})
            ) as procedimento_especialidade,
            upper(trim({{ process_null("procedimento_tipo") }})) as procedimento_tipo,

            upper(left(cid, 3)) as cid_execucao_procedimento,

            -- local solicitacao
            {{ clean_name_string(process_null("unidade_origem_mun")) }}
            as unidade_solicitante_mun,

            lpad(
                safe_cast(unidade_solicitante_cnes as string), 7, '0'
            ) as unidade_solicitante_cnes,

            if(
                lpad(safe_cast(unidade_solicitante_cnes as string), 7, '0') is not null,
                'SIM',
                'NAO'
            ) as unidade_solicitante_preenchida,

            -- local execucao
            {{ clean_name_string(process_null("unidade_executante_mun")) }}
            as unidade_executante_mun,
            lpad(
                safe_cast(unidade_executante_cnes as string), 7, '0'
            ) as unidade_executante_cnes,
            if(
                lpad(safe_cast(unidade_executante_cnes as string), 7, '0') is not null,
                'SIM',
                'NAO'
            ) as unidade_executante_preenchida,

            -- variaveis temporais
            date(data_solicitacao) as data_solicitacao,
            date(data_execucao) as data_execucao,
            date_diff(
                date(data_execucao), date(data_solicitacao), day
            ) as intervalo_solicitacao_execucao,

            -- dados regulacao
            {{ clean_name_string(process_null("solicitacao_status")) }}
            as solicitacao_status,
            {{ clean_name_string(process_null("solicitacao_risco")) }}
            as solicitacao_risco,
            {{ clean_name_string(process_null("central_reguladora")) }}
            as central_reguladora,

        from {{ source("sub_geral_prod", "c34_ser_ambulatorial") }}
        where 1 = 1 and solicitacao_id is not null and data_execucao is not null
    ),

    consolidado as (
        select *
        from source as src

        -- dando inner join ao invés de left pq
        -- aparentemente tinha uns cpfs extras (indesejados) na extração do ser
        inner join {{ ref("raw_projeto_c34__obitos_sim") }} using (paciente_id)
    ),

    enriquecimento as (
        select
            c.*,

            estab_sol.esfera_subgeral as unidade_solicitante_esfera,
            bairros_aps_sol.ap as unidade_solicitante_ap,
            bairros_aps_sol.ap_titulo as unidade_solicitante_ap_descr,
            estab_sol.tipo_unidade_agrupado_subgeral as unidade_solicitante_tp,

            estab_exec.esfera_subgeral as unidade_executante_esfera,
            bairros_aps_exec.ap as unidade_executante_ap,
            bairros_aps_exec.ap_titulo as unidade_executante_ap_descr,
            estab_exec.tipo_unidade_agrupado_subgeral as unidade_executante_tp,

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
            upper(cids_exec.grupo_descricao_abv) as cid_execucao_procedimento_grupo,

            cids_c34_obit.indicador_cancer as obito_causabas_cid_indicador_ca,
            cids_c34_obit.indicador_cancer_pulmao as obito_causabas_cid_indicador_cp,
            upper(cids_obit.categoria_descricao) as obito_causabas_cid_descr,
            upper(cids_obit.grupo_descricao_abv) as obito_causabas_cid_grupo,

            proced_ser.indicador_cancer_pulmao as procedimento_indicador_cp,
            proced_ser.indicador_cancer as procedimento_indicador_ca,

            upper(ibge_res.nome_municipio) as paciente_mun_res_obito,
            upper(ibge_ocor.nome_municipio) as obito_mun_ocor

        from consolidado as c

        -- obtendo dados de estabelecimentos
        left join
            {{ ref("raw_sheets__estabelecimento_auxiliar") }} as estab_sol
            on safe_cast(c.unidade_solicitante_cnes as int64)
            = safe_cast(estab_sol.id_cnes as int64)
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
            {{ ref("dim_estabelecimento_bairro_ap") }} as bairros_aps_sol
            on safe_cast(c.unidade_solicitante_cnes as int64)
            = safe_cast(bairros_aps_sol.id_cnes as int64)
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

        -- classificacao dos procedimentos
        left join
            {{ ref("raw_sheets__projeto_c34_procedimentos_ser") }} as proced_ser
            on c.procedimento = proced_ser.procedimento

    )

select distinct *
from consolidado
