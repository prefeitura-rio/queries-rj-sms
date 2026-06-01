-- Critério 4 (intra-evento) — biópsia no SISREG parada entre datas, em dois
-- legs sequenciais que compartilham critério e peso (distinguidos por
-- `etapa`): leg 1 solicitacao→autorizacao, leg 2 autorizacao→execucao.
-- Cada leg desativa quando a data seguinte é preenchida. Folga: 20 dias por
-- leg. Biópsia no SISREG não tem data_resultado, por isso só dois legs.
{% set criterio_4_intervalo = 20 %}
{% set criterio_4_peso = monitora_cancer_pesos_clinicos()[3] %}

-- O filtro casa os 4 rótulos de biópsia do seed int_monitora_cancer__parametros_sisreg
-- ('Biópsia', 'Biópsia - USG', 'Biópsia - MMG' e 'USG de Mamas - para Biopsia').
-- CONTAINS_SUBSTR normaliza case via NFKC mas preserva diacríticos, por isso
-- 'BIOPSIA' e 'BIÓPSIA' são testados separadamente.

-- leg 1: solicitacao → autorizacao
{{ monitora_cancer_criterio_intra_evento(
    criterio_label='SISREG_BIOPSIA_PROGRESSO',
    intervalo_urgencia=criterio_4_intervalo,
    peso=criterio_4_peso,
    source_filter="fonte = 'SISREG' and (contains_substr(procedimento, 'BIOPSIA') or contains_substr(procedimento, 'BIÓPSIA')) and data_solicitacao is not null and data_autorizacao is null",
    trigger_date_col='data_solicitacao',
    etapa='SOLICITACAO_AUTORIZACAO',
    source_cte_name=ref('int_monitora_cancer__eventos_run_atual')
) }}

union all

-- leg 2: autorizacao → execucao
{{ monitora_cancer_criterio_intra_evento(
    criterio_label='SISREG_BIOPSIA_PROGRESSO',
    intervalo_urgencia=criterio_4_intervalo,
    peso=criterio_4_peso,
    source_filter="fonte = 'SISREG' and (contains_substr(procedimento, 'BIOPSIA') or contains_substr(procedimento, 'BIÓPSIA')) and data_autorizacao is not null and data_execucao is null",
    trigger_date_col='data_autorizacao',
    etapa='AUTORIZACAO_EXECUCAO',
    source_cte_name=ref('int_monitora_cancer__eventos_run_atual')
) }}
