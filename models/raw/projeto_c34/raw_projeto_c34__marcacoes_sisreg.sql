{{ config(materialized="table", schema="projeto_c34", alias="marcacoes_sisreg") }}

with
    sisreg_marcacoes as (
        select
            "SISREG" as sistema,

            to_hex(
                sha256(cast(safe_cast(paciente_cpf as int64) as string))
            ) as paciente_id,

            procedimento_interno_id as procedimento_id,
            upper(left(cid_agendado_id, 3)) as cid_execucao_procedimento,

            unidade_solicitante_id as unidade_solicitante_cnes,
            unidade_executante_id as unidade_executante_cnes,

            date(data_solicitacao) as data_solicitacao,
            date(data_marcacao) as data_execucao,
            date_diff(
                date(data_marcacao), date(data_solicitacao), day
            ) as intervalo_solicitacao_execucao,

            split(solicitacao_status, " / ")[safe_offset(1)] as solicitacao_status,
            solicitacao_risco,

            central_reguladora

        from {{ ref("raw_sisreg_api__marcacoes") }}
        where
            to_hex(sha256(cast(safe_cast(paciente_cpf as int64) as string)))
            in (select paciente_id from {{ ref("raw_projeto_c34__obitos_sim") }})
    ),

    consolidado as (
        select *
        from sisreg_marcacoes
        left join {{ ref("raw_projeto_c34__obitos_sim") }} using (paciente_id)
    ),

    enriquecimento as (
        select
            -- id
            paciente_id,
            sistema,

            -- procedimento executado
            proceds.descricao as procedimento,
            proceds_c34.indicador_cancer_pulmao as procedimento_indicador_cp,
            proceds.especialidade as procedimento_especialidade,
            proceds.tipo_procedimento as procedimento_tipo,

            cid_execucao_procedimento,
            cids_c34_exec.indicador_cancer as cid_execucao_procedimento_indicador_ca,
            cids_c34_exec.indicador_cancer_pulmao
            as cid_execucao_procedimento_indicador_cp,
            upper(cids_exec.categoria_descricao) as cid_execucao_procedimento_descr,
            upper(cids_exec.grupo_descricao_abv) as cid_execucao_procedimento_grupo,

            -- local solicitacao
            case
                when
                    unidade_solicitante_cnes is not null
                    and estab_sol.nome_fantasia is null
                then 'NAO'
                else 'SIM'
            end as unidade_solicitante_mrj_sus,

            estab_sol.nome_fantasia as unidade_solicitante,
            estab_sol.esfera_subgeral as unidade_solicitante_esfera,
            bairros_aps_sol.ap as unidade_solicitante_ap,
            bairros_aps_sol.ap_titulo as unidade_solicitante_ap_descr,

            if(
                unidade_solicitante_cnes is not null, 'SIM', 'NAO'
            ) as unidade_solicitante_preenchida,
            estab_sol.tipo_unidade_agrupado_subgeral as unidade_solicitante_tp,

            -- local execucao
            estab_exec.nome_fantasia as unidade_executante,
            estab_exec.esfera_subgeral as unidade_executante_esfera,
            bairros_aps_exec.ap as unidade_executante_ap,
            bairros_aps_exec.ap_titulo as unidade_executante_ap_descr,

            if(
                unidade_executante_cnes is not null, 'SIM', 'NAO'
            ) as unidade_executante_preenchida,
            estab_exec.tipo_unidade_agrupado_subgeral as unidade_executante_tp,

            -- variaveis temporais
            data_solicitacao,
            data_execucao,
            intervalo_solicitacao_execucao,

            -- dados regulacao
            solicitacao_status,
            solicitacao_risco,
            central_reguladora,

            -- dados paciente
            paciente_cpf_recuperado,
            paciente_mes_obito,
            paciente_sexo,
            paciente_raca_cor,
            upper(ibge_res.nome_municipio) as paciente_mun_res_obito,
            paciente_bairro_res_obito,
            paciente_escolaridade_obito,
            paciente_estado_civil_obito,
            paciente_faixa_etaria_obito,

            obito_causabas_cid,
            cids_c34_obit.indicador_cancer as obito_causabas_cid_indicador_ca,
            cids_c34_obit.indicador_cancer_pulmao as obito_causabas_cid_indicador_cp,
            upper(cids_obit.categoria_descricao) as obito_causabas_cid_descr,
            upper(cids_obit.grupo_descricao_abv) as obito_causabas_cid_grupo,

            -- local ocorrencia obito
            estab_obit.esfera_subgeral as obito_estab_ocor_esfera,
            upper(ibge_ocor.nome_municipio) as obito_mun_ocor,
            bairros_aps_obit.ap as obito_estab_ocor_ap,
            bairros_aps_obit.ap_titulo as obito_estab_ocor_ap_descr,
            estab_obit.tipo_unidade_agrupado_subgeral as obito_estab_ocor_tp,
            if(
                obito_estab_ocor_cnes is not null, 'SIM', 'NAO'
            ) as obito_estab_ocor_preenchido

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

        -- obtendo dados de procedimentos
        left join
            {{ ref("raw_sheets__assistencial_procedimento") }} as proceds
            on safe_cast(c.procedimento_id as int64)
            = safe_cast(proceds.id_procedimento as int64)

        -- classificações de cids c34
        left join
            {{ ref("raw_sheets__projeto_c34_cids") }} as cids_c34_exec
            on c.cid_execucao_procedimento = cids_c34_exec.cid

        left join
            {{ ref("raw_sheets__projeto_c34_cids") }} as cids_c34_obit
            on c.obito_causabas_cid = cids_c34_obit.cid

        -- classificações de procedimentos c34
        left join
            {{ ref("raw_sheets__projeto_c34_procedimentos") }} as proceds_c34
            on c.procedimento_id = proceds_c34.proced_sisreg_id

    )

select *
from enriquecimento
