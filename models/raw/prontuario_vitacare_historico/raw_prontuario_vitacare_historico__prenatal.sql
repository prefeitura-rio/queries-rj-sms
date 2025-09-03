{{
    config(
        alias="pre_natal",
        materialized="incremental",
        unique_key = 'id_prontuario_global',
        cluster_by= 'id_prontuario_global',
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

{% set last_partition = get_last_partition_date(this) %}

WITH

    source_prenatal AS (
        SELECT
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''),
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'prenatal') }}
        {% if is_incremental() %}
            WHERE data_particao > '{{last_partition}}'
        {% endif %}
    ),


      -- Using window function to deduplicate prenata
    prenatal_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_prenatal
        )
        WHERE rn = 1
    ),

    fato_prenatal AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes, 

            SAFE_CAST({{ process_null('param_peso') }} AS NUMERIC) AS param_peso,
            SAFE_CAST({{ process_null('param_tamin') }} AS NUMERIC) AS param_tamin,
            SAFE_CAST({{ process_null('param_tamax') }} AS NUMERIC) AS param_tamax,
            SAFE_CAST({{ process_null('consseguintes_afu') }} AS NUMERIC) AS cons_seguintes_afu,
            {{ process_null('edema') }} AS edema,
            {{ process_null('observacoes') }} AS observacoes,
            SAFE_CAST({{ process_null('pconsulta_peso_habitual') }} AS NUMERIC) AS pconsulta_peso_habitual,
            SAFE_CAST({{ process_null('pconsulta_altura') }} AS NUMERIC) AS pconsulta_altura,
            SAFE_CAST({{ process_null('pconsulta_idade_gestacional') }} AS NUMERIC) AS pconsulta_idade_gestacional,
            {{ process_null('grupo_sanguineo_gravida') }} AS grupo_sanguineo_gravida,
            {{ process_null('pconsulta_grupo_sanguineo_pai') }} AS pconsulta_grupo_sanguineo_pai,
            {{ process_null('consseguintes_afuobs') }} AS cons_seguintes_afu_obs,
            {{ process_null('pelvimetria_clinica') }} AS pelvimetria_clinica,
            {{ process_null('agraval_risco_prenatal_histo_reprod') }} AS agraval_risco_prenatal_histo_reprod,
            {{ process_null('agraval_risco_prenatal_histo_obstet_anterior') }} AS agraval_risco_prenatal_histo_obstet_anterior,
            {{ process_null('agraval_risco_prenatal_patol_associada') }} AS agraval_risco_prenatal_patol_associada,
            {{ process_null('agraval_risco_prenatal_gravidez_actual') }} AS agraval_risco_prenatal_gravidez_actual,
            SAFE_CAST({{ process_null('aval_risco_prenatal_total_indices') }} AS NUMERIC) AS aval_risco_prenatal_total_indices,
            {{ process_null('agraval_risco_prenatal_escala_nunomont') }} AS agraval_risco_prenatal_escala_nunomont,
            SAFE_CAST({{ process_null('exames_lab_glicemia_pos_50') }} AS NUMERIC) AS exames_lab_glicemia_pos_50,
            {{ process_null('exames_lab_tcoombs') }} AS exames_lab_tcoombs,
            SAFE_CAST({{ process_null('exames_lab_glicemia_jejum') }} AS NUMERIC) AS exames_lab_glicemia_jejum,
            SAFE_CAST({{ process_null('exames_lab_ptgo_0') }} AS NUMERIC) AS exames_lab_ptgo_0,
            SAFE_CAST({{ process_null('exames_lab_ptgo_60') }} AS NUMERIC) AS exames_lab_ptgo_60,
            SAFE_CAST({{ process_null('exames_lab_ptg_120') }} AS NUMERIC) AS exames_lab_ptg_120,
            SAFE_CAST({{ process_null('exames_lab_ptg_180') }} AS NUMERIC) AS exames_lab_ptg_180,
            SAFE_CAST({{ process_null('exames_lab_creatinemia') }} AS NUMERIC) AS exames_lab_creatinemia,
            {{ process_null('exames_lab_vdrl') }} AS exames_lab_vdrl,
            {{ process_null('exames_lab_toxoplasmose_imun') }} AS exames_lab_toxoplasmose_imun,
            SAFE_CAST({{ process_null('exames_lab_rubeola_igg') }} AS NUMERIC) AS exames_lab_rubeola_igg,
            SAFE_CAST({{ process_null('exames_lab_rubeola_igm') }} AS NUMERIC) AS exames_lab_rubeola_igm,
            {{ process_null('exames_lab_rubeola_imun') }} AS exames_lab_rubeola_imun,
            SAFE_CAST({{ process_null('exames_lab_aghbs') }} AS NUMERIC) AS exames_lab_aghbs,
            {{ process_null('exames_lab_hiv') }} AS exames_lab_hiv,
            {{ process_null('exames_lab_urocultura') }} AS exames_lab_urocultura,
            {{ process_null('exames_lab_urocultura_positiva') }} AS exames_lab_urocultura_positiva,
            {{ process_null('exame_clinic_mama_esq_inspeccao') }} AS exame_clinico_mama_esq_inspeccao,
            {{ process_null('exame_clini_cmama_esq_palpacao') }} AS exame_clinico_mama_esq_palpacao,
            {{ process_null('exame_clinic_mama_dir_inspeccao') }} AS exame_clinico_mama_dir_inspeccao,
            {{ process_null('exame_clinic_mama_dir_palpacao') }} AS exame_clinico_mama_dir_palpacao,
            {{ process_null('exame_ginec_vulva_vagina') }} AS exame_ginec_vulva_vagina,
            {{ process_null('colo_utero_exame') }} AS colo_utero_exame,
            {{ process_null('colo_utero_toque') }} AS colo_utero_toque,
            {{ process_null('exame_citologiares') }} AS exame_citologia_res,
            SAFE_CAST({{ process_null('pconsulta_dum1') }} AS NUMERIC) AS pconsulta_dum1,
            {{ process_null('pconsulta_dpp1') }} AS pconsulta_dpp1,
            {{ process_null('agraval_risco_prenatal_obs') }} AS agraval_risco_prenatal_obs,
            SAFE_CAST({{ process_null('pconsulta_idade_gestacional_dias') }} AS NUMERIC) AS pconsulta_idade_gestacional_dias,
            {{ process_null('exame_clinic_mama_esq_inspeccao_obs') }} AS exame_clinico_mama_esq_inspeccao_obs,
            {{ process_null('exame_clinic_mama_dir_inspeccao_obs') }} AS exame_clinico_mama_dir_inspeccao_obs,
            {{ process_null('exame_clinic_mama_esq_palpacao_obs') }} AS exame_clinico_mama_esq_palpacao_obs,
            {{ process_null('exame_clinic_mama_dir_palpacao_obs') }} AS exame_clinico_mama_dir_palpacao_obs,
            {{ process_null('queixas_mam') }} AS queixas_mam,
            SAFE_CAST({{ process_null('analises_eritrocitos') }} AS NUMERIC) AS analises_eritrocitos,
            SAFE_CAST({{ process_null('analises_hemoglobina') }} AS NUMERIC) AS analises_hemoglobina,
            SAFE_CAST({{ process_null('analises_hematocrito') }} AS NUMERIC) AS analises_hematocrito,
            SAFE_CAST({{ process_null('analises_gm') }} AS NUMERIC) AS analises_gm,
            SAFE_CAST({{ process_null('analises_hgm') }} AS NUMERIC) AS analises_hgm,
            SAFE_CAST({{ process_null('analises_chgm') }} AS NUMERIC) AS analises_chgm,
            SAFE_CAST({{ process_null('analises_leucocitos') }} AS NUMERIC) AS analises_leucocitos,
            SAFE_CAST({{ process_null('analises_neutrifilos') }} AS NUMERIC) AS analises_neutrifilos,
            SAFE_CAST({{ process_null('analises_eosinofilos') }} AS NUMERIC) AS analises_eosinofilos,
            SAFE_CAST({{ process_null('analises_basofilos') }} AS NUMERIC) AS analises_basofilos,
            SAFE_CAST({{ process_null('analises_linfocitos') }} AS NUMERIC) AS analises_linfocitos,
            SAFE_CAST({{ process_null('analises_monocitos') }} AS NUMERIC) AS analises_monocitos,
            SAFE_CAST({{ process_null('cseguintes_alalises_plaquetas') }} AS NUMERIC) AS cons_seguintes_analises_plaquetas,
            SAFE_CAST({{ process_null('cseguintes_alalises_acidourico') }} AS NUMERIC) AS cons_seguintes_analises_acidourico,
            {{ process_null('cseguintes_alalises_citomegalo_virus') }} AS cons_seguintes_analises_citomegalo_virus,
            SAFE_CAST({{ process_null('cseguintes_alalises_achbs') }} AS NUMERIC) AS cons_seguintes_analises_achbs,
            SAFE_CAST({{ process_null('cseguintes_alalises_achbc') }} AS NUMERIC) AS cons_seguintes_analises_achbc,
            SAFE_CAST({{ process_null('cseguintes_alalises_achvc') }} AS NUMERIC) AS cons_seguintes_analises_achvc,
            SAFE_CAST({{ process_null('cseguintes_alalises_alfa_proteina') }} AS NUMERIC) AS cons_seguintes_analises_alfa_proteina,
            SAFE_CAST({{ process_null('cseguintes_alalises_inibinaa') }} AS NUMERIC) AS cons_seguintes_analises_inibinaa,
            SAFE_CAST({{ process_null('cseguintes_alalises_estradiol') }} AS NUMERIC) AS cons_seguintes_analises_estradiol,
            SAFE_CAST({{ process_null('cseguintes_alalises_hormona_beta_corionica') }} AS NUMERIC) AS cons_seguintes_analises_hormona_beta_corionica,
            SAFE_CAST({{ process_null('cseguintes_alalises_tempo_protrombina') }} AS NUMERIC) AS cons_seguintes_analises_tempo_protrombina,
            SAFE_CAST({{ process_null('cseguintes_alalises_ttrombo_plastina_parcial') }} AS NUMERIC) AS cons_seguintes_analises_ttrombo_plastina_parcial,
            {{ process_null('cseguintes_alalises_ecografia_obstetrica') }} AS cons_seguintes_analises_ecografia_obstetrica,
            {{ process_null('alalises_eco_obstet_morfologica') }} AS analises_eco_obstet_morfologica,
            {{ process_null('exame_citologia_resrccu') }} AS exame_citologia_res_rccu,
            SAFE_CAST({{ process_null('pconsulta_dum1dias') }} AS NUMERIC) AS pconsulta_dum1_dias,
            {{ process_null('data_mamografia') }} AS data_mamografia,
            {{ process_null('res_mamografias') }} AS res_mamografias,
            {{ process_null('analises_combur_combur') }} AS analises_combur_combur,
            {{ process_null('analises_combur_bilirrubina') }} AS analises_combur_bilirrubina,
            {{ process_null('analises_combur_urobilogenio') }} AS analises_combur_urobilogenio,
            {{ process_null('analises_combur_cetonas') }} AS analises_combur_cetonas,
            {{ process_null('analises_combur_glucose') }} AS analises_combur_glucose,
            {{ process_null('analises_combur_proteinas') }} AS analises_combur_proteinas,
            {{ process_null('analises_combur_sangue') }} AS analises_combur_sangue,
            {{ process_null('analises_combur_nitritos') }} AS analises_combur_nitritos,
            {{ process_null('analises_combur_ph') }} AS analises_combur_ph,
            {{ process_null('analises_combur_densidade') }} AS analises_combur_densidade,
            {{ process_null('analises_combur_leucocitos') }} AS analises_combur_leucocitos,
            {{ process_null('exames_lab_urocultur_ancolonias') }} AS exames_lab_urocultur_ancolonias,
            {{ process_null('data_resultado_citologia') }} AS data_resultado_citologia,
            SAFE_CAST({{ process_null('pconsulta_tamax') }} AS NUMERIC) AS pconsulta_tamax,
            SAFE_CAST({{ process_null('pconsulta_tamin') }} AS NUMERIC) AS pconsulta_tamin,
            {{ process_null('dta_eco') }} AS dta_eco,
            {{ process_null('res_mamografias_obs') }} AS res_mamografias_obs,
            SAFE_CAST({{ process_null('cons_seguintes_perimetro_abdominal') }} AS NUMERIC) AS cons_seguintes_perimetro_abdominal,
            SAFE_CAST({{ process_null('primeira_consulta_idade_gestacional') }} AS NUMERIC) AS primeira_consulta_idade_gestacional,
            SAFE_CAST({{ process_null('primeira_consulta_idade_gestacional_dias') }} AS NUMERIC) AS primeira_consulta_idade_gestacional_dias,
            {{ process_null('res_exames_lab_toxoplasmose_igg') }} AS res_exames_lab_toxoplasmose_igg,
            {{ process_null('res_exames_lab_toxoplasmose_igm') }} AS res_exames_lab_toxoplasmose_igm,
            {{ process_null('exames_lab_cultura_ag_patologicos') }} AS exames_lab_cultura_ag_patologicos,
            {{ process_null('exames_lab_cultura_tsa_sensiveis') }} AS exames_lab_cultura_tsa_sensiveis,
            {{ process_null('exames_lab_cultura_tsa_resistentes') }} AS exames_lab_cultura_tsa_resistentes,
            {{ process_null('exame_ginec_vulva_vagina_outras') }} AS exame_ginec_vulva_vagina_outras,
            {{ process_null('colo_utero_toque_outras') }} AS colo_utero_toque_outras,
            {{ process_null('classificacao_gestacao_alto_risco') }} AS classificacao_gestacao_alto_risco,
            SAFE_CAST({{ process_null('gestante_gestacoes_num') }} AS NUMERIC) AS gestante_gestacoes_num,
            {{ process_null('sifilis_tratamento_dose_1') }} AS sifilis_tratamento_dose_1,
            {{ process_null('sifilis_tratamento_dose_2') }} AS sifilis_tratamento_dose_2,
            {{ process_null('sifilis_tratamento_dose_3') }} AS sifilis_tratamento_dose_3,
            {{ process_null('sifilis_tratamento_de_parceiro') }} AS sifilis_tratamento_de_parceiro,
            {{ process_null('sifilis_inc_ou_efetuado_tratamento_parceiro_dose_1') }} AS sifilis_inc_ou_efetuado_tratamento_parceiro_dose_1,
            {{ process_null('sifilis_inc_ou_efetuado_tratamento_parceiro_dose_2') }} AS sifilis_inc_ou_efetuado_tratamento_parceiro_dose_2,
            {{ process_null('sifilis_inc_ou_efetuado_tratamento_parceiro_dose_3') }} AS sifilis_inc_ou_efetuado_tratamento_parceiro_dose_3,
            {{ process_null('sifilis_data_visita_domiciliar') }} AS sifilis_data_visita_domiciliar,
            {{ process_null('sifilis_motivo_visita_domiciliar') }} AS sifilis_motivo_visita_domiciliar,
            {{ process_null('sifilis_observacoes') }} AS sifilis_observacoes,
            {{ process_null('exames_lab_streptoccus_b') }} AS exames_lab_streptoccus_b,
            {{ process_null('exames_lab_streptoccus_b_obs') }} AS exames_lab_streptoccus_b_obs,
            {{ process_null('consulta_seguintes_intercorrencias') }} AS consulta_seguintes_intercorrencias,
            SAFE_CAST({{ process_null('exames_lab_hemoglobina_d') }} AS NUMERIC) AS exames_lab_hemoglobina_d,
            {{ process_null('exames_lab_vdrl_titulacao') }} AS exames_lab_vdrl_titulacao,
            {{ process_null('sifilis_teste_rapido_sifilis_parceiro') }} AS sifilis_teste_rapido_sifilis_parceiro,
            {{ process_null('sifilis_vdrl_parceiro') }} AS sifilis_vdrl_parceiro,
            {{ process_null('cons_seguintes_placenta_posicionamento') }} AS cons_seguintes_placenta_posicionamento,
            {{ process_null('cons_seguintes_placenta_sinais_risco') }} AS cons_seguintes_placenta_sinais_risco,
            {{ process_null('cons_seguintes_placenta_maturidade') }} AS cons_seguintes_placenta_maturidade,
            {{ process_null('cons_seguintes_placenta_observacoes') }} AS cons_seguintes_placenta_observacoes,
            {{ process_null('cons_seguintes_liquido_amniotico') }} AS cons_seguintes_liquido_amniotico,
            {{ process_null('cons_seguintes_liquido_amniotico_observacoes') }} AS cons_seguintes_liquido_amniotico_observacoes,
            {{ process_null('cseguintes_cardiotocografia') }} AS cons_seguintes_cardiotocografia,
            {{ process_null('sifilis_titulacao_parceiro') }} AS sifilis_titulacao_parceiro,
            {{ process_null('sifilis_esquema_tratamento_parceiro') }} AS sifilis_esquema_tratamento_parceiro,
            {{ process_null('exames_lab_agr_testes_rapidos_hiv') }} AS exames_lab_agr_testes_rapidos_hiv,
            {{ process_null('exames_lab_agr_testes_rapidos_hiv_positivo') }} AS exames_lab_agr_testes_rapidos_hiv_positivo,
            {{ process_null('smrpuerperiodata') }} AS sm_puerperio_data,
            {{ process_null('smlocalparto') }} AS sm_local_parto,
            {{ process_null('smtipoparto') }} AS sm_tipo_parto,
            {{ process_null('smresivaopuerpobs') }} AS sm_res_ivaopuerpo_obs,
            SAFE_CAST({{ process_null('smpuerperioparametrospeso') }} AS NUMERIC) AS sm_puerperio_parametros_peso,
            SAFE_CAST({{ process_null('smpuerperioparametrostamin') }} AS NUMERIC) AS sm_puerperio_parametros_tamin,
            SAFE_CAST({{ process_null('smpuerperioparametrostamax') }} AS NUMERIC) AS sm_puerperio_parametros_tamax,
            {{ process_null('smpuerperiocitologia') }} AS sm_puerperio_citologia,
            SAFE_CAST({{ process_null('smpuerperioalalisesplaquetas') }} AS NUMERIC) AS sm_puerperio_analises_plaquetas,
            SAFE_CAST({{ process_null('smpuerperioalalisesglicemia') }} AS NUMERIC) AS sm_puerperio_analises_glicemia,
            SAFE_CAST({{ process_null('smpuerperioalalisescreatinina') }} AS NUMERIC) AS sm_puerperio_analises_creatinina,
            SAFE_CAST({{ process_null('smpuerperioalalisesgamagt') }} AS NUMERIC) AS sm_puerperio_analises_gamagt,
            SAFE_CAST({{ process_null('smpuerperioalalisesgot') }} AS NUMERIC) AS sm_puerperio_analises_got,
            SAFE_CAST({{ process_null('smpuerperioalalisesgtp') }} AS NUMERIC) AS sm_puerperio_analises_gtp,
            {{ process_null('smpuerperioalalisesbilirrubinas') }} AS sm_puerperio_analises_bilirrubinas,
            SAFE_CAST({{ process_null('smpuerperioalaliseacidourico') }} AS NUMERIC) AS sm_puerperio_analise_acidourico,
            {{ process_null('smpuerperiourocultura') }} AS sm_puerperio_urocultura,
            {{ process_null('classificacaotipoparto') }} AS classificacao_tipo_parto,
            SAFE_CAST({{ process_null('puerperiomcdtsfosfatasealcalina') }} AS NUMERIC) AS puerperio_mcdts_fosfatase_alcalina,
            {{ process_null('puerperiomcdtsurinai') }} AS puerperio_mcdts_urinai,
            {{ process_null('puerperiomcdtsurinaiobs') }} AS puerperio_mcdts_urinai_obs,
            {{ process_null('puerperiourocultura') }} AS puerperio_urocultura,
            {{ process_null('puerperionumcolonias') }} AS puerperio_num_colonias,
            {{ process_null('puerperioagentespatologicos') }} AS puerperio_agentes_patologicos,
            {{ process_null('puerperiotsasensiveis') }} AS puerperio_tsa_sensiveis,
            {{ process_null('puerperiotsaresistentes') }} AS puerperio_tsa_resistentes,
            {{ process_null('puerperiomcdtsurinairesulltado') }} AS puerperio_mcdts_urinai_resultado,
            {{ process_null('puerperiotermo') }} AS puerperio_termo,
            {{ process_null('puerperiorazaocesareo') }} AS puerperio_razao_cesareo,
            {{ process_null('puerperiorh') }} AS puerperio_rh,
            {{ process_null('puerperiogamaglobulinaantid') }} AS puerperio_gama_globulina_antid,
            {{ process_null('puerperiogamaglobulinaantiddata') }} AS puerperio_gama_globulina_antid_data,
            SAFE_CAST({{ process_null('gestantepuerperionumrecemnascidos') }} AS NUMERIC) AS gestante_puerperio_num_recem_nascidos,
            {{ process_null('gestantepuerperioamamentacao') }} AS gestante_puerperio_amamentacao,
            {{ process_null('puerperiodataaborto') }} AS puerperio_data_aborto,
            {{ process_null('puerperiotipoaborto') }} AS puerperio_tipo_aborto,
            SAFE_CAST({{ process_null('puerperioidadegestacao') }} AS NUMERIC) AS puerperio_idade_gestacao,
            {{ process_null('gestantepuerperiocuretagem') }} AS gestante_puerperio_curetagem,
            {{ process_null('puerperiooutrotermoqual') }} AS puerperio_outro_termo_qual,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao

        FROM prenatal_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_prenatal
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado