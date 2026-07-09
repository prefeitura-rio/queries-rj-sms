-- Critério 6 (intra-status) — solicitação SER travada no status "EM_FILA".
-- O próprio filtro de status desativa: quando o status muda, a linha some.
-- Folga: 60 dias. Emite a relação canônica bruta de 8 colunas.
{% set criterio_6_intervalo = 60 %}
{% set criterio_6_peso = monitora_cancer_pesos_clinicos()[5] %}

{{ monitora_cancer_criterio_intra_evento(
    criterio_label='SER_EM_FILA__STATUS_UPDATE',
    intervalo_urgencia=criterio_6_intervalo,
    peso=criterio_6_peso,
    source_filter="fonte = 'SER' and evento_status = 'EM_FILA' and data_solicitacao is not null",
    trigger_date_col='data_solicitacao',
    source_cte_name=ref('int_monitora_cancer__eventos_run_atual')
) }}
