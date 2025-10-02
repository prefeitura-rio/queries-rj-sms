{{
    config(
        enabled=true,
        alias="sifilis_gestantes",
    )
}}

WITH
    -- Etapa 1: Obter a base de gestantes ATIVAS.
    base_gestantes AS (
        SELECT DISTINCT
            id_gestacao,
            id_paciente,
            cpf,
            SAFE.PARSE_DATE (
                '%Y-%m-%d',
                SUBSTR(
                    CAST(data_inicio AS STRING),
                    1,
                    10
                )
            ) AS data_inicio_gestacao,
            COALESCE(
                SAFE.PARSE_DATE (
                    '%Y-%m-%d',
                    SUBSTR(
                        CAST(data_fim_efetiva AS STRING),
                        1,
                        10
                    )
                ),
                CURRENT_DATE('America/Sao_Paulo')
            ) AS data_fim_gestacao
        FROM
            {{ ref('mart_bi_gestacoes__gestacoes') }}
        WHERE
            fase_atual = 'Gestação'
    ),
    -- Etapa 2: Unificar e preparar todas as fontes de dados brutos.
    diagnosticos_raw AS (
        -- Fonte 1: Tabela episodio_assistencial (dados já estruturados)
        SELECT DISTINCT
            paciente.id_paciente,
            cond.id AS cid,
            SAFE.PARSE_DATE (
                '%Y-%m-%d',
                SUBSTR(
                    CAST(
                        cond.data_diagnostico AS STRING
                    ),
                    1,
                    10
                )
            ) AS data_diagnostico
        FROM
            {{ ref('mart_historico_clinico__episodio') }},
            UNNEST (condicoes) AS cond
        WHERE (
                cond.id LIKE 'A51%'
                OR cond.id LIKE 'A53%'
                OR cond.id LIKE 'O981'
            )
            AND cond.situacao = 'ATIVO'
        UNION ALL
        -- Fonte 2: Tabela atendimento (dados brutos em JSON)
        SELECT DISTINCT
            atend.cpf AS id_paciente,
            JSON_EXTRACT_SCALAR (cond_json, '$.cod_cid10') AS cid,
            SAFE.PARSE_DATE (
                '%Y-%m-%d',
                SUBSTR(
                    JSON_EXTRACT_SCALAR (
                        cond_json,
                        '$.data_diagnostico'
                    ),
                    1,
                    10
                )
            ) AS data_diagnostico
        FROM
            {{ ref('raw_prontuario_vitacare__atendimento') }} AS atend,
            UNNEST (
                JSON_QUERY_ARRAY (atend.condicoes)
            ) AS cond_json
        WHERE
            JSON_EXTRACT_SCALAR (cond_json, '$.estado') = 'ATIVO'
            AND (
                JSON_EXTRACT_SCALAR (cond_json, '$.cod_cid10') LIKE 'A51%'
                OR JSON_EXTRACT_SCALAR (cond_json, '$.cod_cid10') LIKE 'A53%'
                OR JSON_EXTRACT_SCALAR (cond_json, '$.cod_cid10') LIKE 'O981%'
            )
    ),
    vdrl_raw AS (
        SELECT DISTINCT
            acto.patient_cpf AS cpf,
            SAFE.PARSE_DATE (
                '%Y-%m-%d',
                SUBSTR(
                    CAST(
                        acto.datahora_inicio_atendimento AS STRING
                    ),
                    1,
                    10
                )
            ) as data_exame,
            prenatal.exames_lab_vdrl AS resultado,
            prenatal.exames_lab_vdrl_titulacao AS titulacao
        FROM
            {{ ref('raw_prontuario_vitacare_historico__prenatal') }} AS prenatal
            INNER JOIN {{ ref('raw_prontuario_vitacare_historico__acto') }} AS acto ON
        REPLACE (
                CAST(
                    prenatal.id_prontuario_global AS STRING
                ),
                '.0',
                ''
            ) =
        REPLACE (
                CAST(
                    acto.id_prontuario_global AS STRING
                ),
                '.0',
                ''
            )
        WHERE
            prenatal.exames_lab_vdrl IS NOT NULL
            OR prenatal.exames_lab_vdrl_titulacao IS NOT NULL
    ),
    -- ####################################################################
    -- CTE CORRIGIDA E SIMPLIFICADA para extrair os dados mais recentes do parceiro
    -- ####################################################################
    dados_parceiro_raw AS (
        SELECT
            cpf,
            sifilis_tratamento_de_parceiro,
            sifilis_inc_ou_efetuado_tratamento_parceiro_dose_1,
            sifilis_inc_ou_efetuado_tratamento_parceiro_dose_2,
            sifilis_inc_ou_efetuado_tratamento_parceiro_dose_3,
            sifilis_teste_rapido_sifilis_parceiro,
            sifilis_vdrl_parceiro,
            sifilis_titulacao_parceiro,
            sifilis_esquema_tratamento_parceiro
        FROM (
                SELECT acto.patient_cpf AS cpf, prenatal.sifilis_tratamento_de_parceiro, prenatal.sifilis_inc_ou_efetuado_tratamento_parceiro_dose_1, prenatal.sifilis_inc_ou_efetuado_tratamento_parceiro_dose_2, prenatal.sifilis_inc_ou_efetuado_tratamento_parceiro_dose_3, prenatal.sifilis_teste_rapido_sifilis_parceiro, prenatal.sifilis_vdrl_parceiro, prenatal.sifilis_titulacao_parceiro, prenatal.sifilis_esquema_tratamento_parceiro,
                    -- CORREÇÃO: Usando a data da tabela 'acto' diretamente.
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            acto.patient_cpf
                        ORDER BY acto.datahora_inicio_atendimento DESC
                    ) as rn
                FROM
                    {{ ref('raw_prontuario_vitacare_historico__prenatal') }} AS prenatal
                    INNER JOIN {{ ref('raw_prontuario_vitacare_historico__acto') }} AS acto ON
                REPLACE (
                        CAST(
                            prenatal.id_prontuario_global AS STRING
                        ), '.0', ''
                    ) =
                REPLACE (
                        CAST(
                            acto.id_prontuario_global AS STRING
                        ), '.0', ''
                    )
            )
        WHERE
            rn = 1
    ),
    -- Etapa 3: Identificar CICLOS de tratamento em 3 PASSOS.
    dispensacoes_com_lag AS (
        SELECT
            g.id_gestacao,
            g.id_paciente,
            g.cpf,
            d.data_dispensacao,
            g.data_inicio_gestacao,
            LAG(d.data_dispensacao, 1) OVER (
                PARTITION BY
                    g.id_gestacao
                ORDER BY d.data_dispensacao
            ) AS data_dispensacao_anterior
        FROM base_gestantes AS g
            JOIN (
                SELECT DISTINCT
                    consumo_paciente_cpf, SAFE.PARSE_DATE (
                        '%Y-%m-%d', SUBSTR(
                            CAST(data_evento AS STRING), 1, 10
                        )
                    ) AS data_dispensacao
                FROM
                    {{ ref('mart_estoque__movimento') }}
                WHERE
                    LOWER(material_descricao) LIKE '%benzilpenicilina%benzatina%'
            ) AS d ON g.cpf = d.consumo_paciente_cpf
        WHERE
            d.data_dispensacao BETWEEN g.data_inicio_gestacao AND g.data_fim_gestacao
    ),
    dispensacoes_com_flag AS (
        SELECT
            *,
            CASE
                WHEN data_dispensacao_anterior IS NULL THEN 1
                WHEN DATE_DIFF (
                    data_dispensacao,
                    data_dispensacao_anterior,
                    DAY
                ) > 21 THEN 1
                ELSE 0
            END AS is_new_cycle
        FROM dispensacoes_com_lag
    ),
    dispensacoes_com_ciclo AS (
        SELECT
            id_gestacao,
            id_paciente,
            cpf,
            data_dispensacao,
            data_inicio_gestacao,
            SUM(is_new_cycle) OVER (
                PARTITION BY
                    id_gestacao
                ORDER BY data_dispensacao
            ) AS ciclo_tratamento_id
        FROM dispensacoes_com_flag
    ),
    -- Etapa 4: Agrupar as dispensações por ciclo.
    ciclos_de_tratamento AS (
        SELECT
            id_gestacao,
            id_paciente,
            cpf,
            ciclo_tratamento_id,
            ANY_VALUE(data_inicio_gestacao) AS data_inicio_gestacao,
            MIN(
                CASE
                    WHEN dose_num = 1 THEN data_dispensacao
                END
            ) AS data_dispensacao_dose_1,
            MIN(
                CASE
                    WHEN dose_num = 2 THEN data_dispensacao
                END
            ) AS data_dispensacao_dose_2,
            MIN(
                CASE
                    WHEN dose_num = 3 THEN data_dispensacao
                END
            ) AS data_dispensacao_dose_3,
            MAX(data_dispensacao) AS data_ultima_dispensacao_ciclo,
            COUNT(data_dispensacao) AS numero_doses_dispensadas
        FROM (
                SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY
                            id_gestacao, ciclo_tratamento_id
                        ORDER BY data_dispensacao
                    ) AS dose_num
                FROM dispensacoes_com_ciclo
            )
        GROUP BY
            id_gestacao,
            id_paciente,
            cpf,
            ciclo_tratamento_id
    ),
    -- Etapa 5: Associar os VDRLs de diagnóstico e acompanhamento
    vdrl_associado AS (
        SELECT
            ct.id_gestacao,
            ct.ciclo_tratamento_id,
            MAX(
                CASE
                    WHEN v.data_exame <= ct.data_dispensacao_dose_1 THEN v.data_exame
                END
            ) AS vdrl_diagnostico_data,
            MIN(
                CASE
                    WHEN v.data_exame > ct.data_ultima_dispensacao_ciclo
                    AND DATE_DIFF (
                        v.data_exame,
                        ct.data_ultima_dispensacao_ciclo,
                        DAY
                    ) >= 30 THEN v.data_exame
                END
            ) AS vdrl_acompanhamento_data
        FROM
            ciclos_de_tratamento ct
            JOIN vdrl_raw v ON ct.cpf = v.cpf
        GROUP BY
            ct.id_gestacao,
            ct.ciclo_tratamento_id
    )
    -- Etapa 6: Montagem final
SELECT
    ct.id_gestacao,
    ct.id_paciente,
    FARM_FINGERPRINT (
        CONCAT(
            ct.id_paciente,
            CAST(
                diag.data_diagnostico AS STRING
            )
        )
    ) AS id_episodio_sifilis,
    ct.ciclo_tratamento_id,
    diag.data_diagnostico AS data_diagnostico_associado,
    CASE
        WHEN diag.data_diagnostico IS NOT NULL THEN 'Sim'
        ELSE 'Não'
    END AS diagnostico_associado,
    DATE_DIFF (
        ct.data_dispensacao_dose_1,
        diag.data_diagnostico,
        DAY
    ) AS dias_diag_para_disp,
    DATE_DIFF (
        ct.data_dispensacao_dose_1,
        ct.data_inicio_gestacao,
        WEEK
    ) AS ig_inicio_tratamento_semanas,
    ct.data_dispensacao_dose_1,
    ct.data_dispensacao_dose_2,
    ct.data_dispensacao_dose_3,
    ct.numero_doses_dispensadas,
    -- Dados do VDRL da Gestante
    vdrl.vdrl_diagnostico_data,
    diag_vdrl.resultado AS vdrl_diagnostico_resultado,
    diag_vdrl.titulacao AS vdrl_diagnostico_titulacao,
    vdrl.vdrl_acompanhamento_data,
    acomp_vdrl.resultado AS vdrl_acompanhamento_resultado,
    acomp_vdrl.titulacao AS vdrl_acompanhamento_titulacao,
    -- Dados do Parceiro
    parceiro.sifilis_tratamento_de_parceiro,
    parceiro.sifilis_inc_ou_efetuado_tratamento_parceiro_dose_1,
    parceiro.sifilis_inc_ou_efetuado_tratamento_parceiro_dose_2,
    parceiro.sifilis_inc_ou_efetuado_tratamento_parceiro_dose_3,
    parceiro.sifilis_teste_rapido_sifilis_parceiro,
    parceiro.sifilis_vdrl_parceiro,
    parceiro.sifilis_titulacao_parceiro,
    parceiro.sifilis_esquema_tratamento_parceiro,

-- Colunas de Status (KPIs)
CASE
    WHEN ct.numero_doses_dispensadas >= 3 THEN 'Completo (3+ doses)'
    WHEN ct.numero_doses_dispensadas = 2
    AND DATE_DIFF (
        CURRENT_DATE('America/Sao_Paulo'),
        ct.data_dispensacao_dose_2,
        DAY
    ) <= 14 THEN 'Em curso (aguardando D3)'
    WHEN ct.numero_doses_dispensadas = 2
    AND DATE_DIFF (
        CURRENT_DATE('America/Sao_Paulo'),
        ct.data_dispensacao_dose_2,
        DAY
    ) > 14 THEN 'Incompleto (2 doses)'
    WHEN ct.numero_doses_dispensadas = 1
    AND DATE_DIFF (
        CURRENT_DATE('America/Sao_Paulo'),
        ct.data_dispensacao_dose_1,
        DAY
    ) <= 14 THEN 'Em curso (aguardando D2)'
    WHEN ct.numero_doses_dispensadas = 1
    AND DATE_DIFF (
        CURRENT_DATE('America/Sao_Paulo'),
        ct.data_dispensacao_dose_1,
        DAY
    ) > 14 THEN 'Incompleto (1 dose)'
    ELSE 'Verificar'
END AS status_tratamento_dispensado,

-- Status Final focado na GESTANTE
CASE
    WHEN diag.data_diagnostico IS NULL THEN 'ALERTA: Tratamento sem Diagnóstico'
    WHEN ct.numero_doses_dispensadas < 3
    AND (
        (
            ct.numero_doses_dispensadas = 1
            AND DATE_DIFF (
                CURRENT_DATE('America/Sao_Paulo'),
                ct.data_dispensacao_dose_1,
                DAY
            ) > 14
        )
        OR (
            ct.numero_doses_dispensadas = 2
            AND DATE_DIFF (
                CURRENT_DATE('America/Sao_Paulo'),
                ct.data_dispensacao_dose_2,
                DAY
            ) > 14
        )
    ) THEN 'FALHA: Tratamento da Gestante Incompleto'
    WHEN (
        ct.data_dispensacao_dose_2 IS NOT NULL
        AND NOT(
            DATE_DIFF (
                ct.data_dispensacao_dose_2,
                ct.data_dispensacao_dose_1,
                DAY
            ) BETWEEN 7 AND 9
        )
    )
    OR (
        ct.data_dispensacao_dose_3 IS NOT NULL
        AND NOT(
            DATE_DIFF (
                ct.data_dispensacao_dose_3,
                ct.data_dispensacao_dose_2,
                DAY
            ) BETWEEN 7 AND 9
        )
    ) THEN 'FALHA: Intervalo entre doses incorreto'
    WHEN ct.numero_doses_dispensadas < 3 THEN 'ACOMPANHAR: Tratamento em Curso'
    WHEN ct.numero_doses_dispensadas >= 1
    AND vdrl.vdrl_acompanhamento_data IS NULL THEN 'PENDÊNCIA: Monitoramento de Cura'
    ELSE 'Cuidado Adequado (Gestante)'
END AS status_final_gestante,

-- Status Final focado no PARCEIRO
CASE
    WHEN parceiro.sifilis_tratamento_de_parceiro = 'Sim' THEN 'Tratado'
    WHEN parceiro.sifilis_tratamento_de_parceiro = 'Não' THEN 'FALHA: Não Tratado'
    ELSE 'Sem Informação'
END AS status_parceiro
FROM
    ciclos_de_tratamento AS ct
    LEFT JOIN (
        SELECT ct.id_gestacao, ct.ciclo_tratamento_id, MAX(diag.data_diagnostico) as data_diagnostico
        FROM
            ciclos_de_tratamento ct
            JOIN base_gestantes gest ON ct.id_gestacao = gest.id_gestacao
            JOIN diagnosticos_raw diag ON (
                gest.id_paciente = diag.id_paciente
                OR gest.cpf = diag.id_paciente
            )
            AND diag.data_diagnostico <= ct.data_dispensacao_dose_1
        GROUP BY
            ct.id_gestacao,
            ct.ciclo_tratamento_id
    ) AS diag ON ct.id_gestacao = diag.id_gestacao
    AND ct.ciclo_tratamento_id = diag.ciclo_tratamento_id
    LEFT JOIN vdrl_associado AS vdrl ON ct.id_gestacao = vdrl.id_gestacao
    AND ct.ciclo_tratamento_id = vdrl.ciclo_tratamento_id
    LEFT JOIN vdrl_raw AS diag_vdrl ON ct.cpf = diag_vdrl.cpf
    AND vdrl.vdrl_diagnostico_data = diag_vdrl.data_exame
    LEFT JOIN vdrl_raw AS acomp_vdrl ON ct.cpf = acomp_vdrl.cpf
    AND vdrl.vdrl_acompanhamento_data = acomp_vdrl.data_exame
    LEFT JOIN dados_parceiro_raw AS parceiro ON ct.cpf = parceiro.cpf
ORDER BY ct.id_paciente, ct.data_dispensacao_dose_1