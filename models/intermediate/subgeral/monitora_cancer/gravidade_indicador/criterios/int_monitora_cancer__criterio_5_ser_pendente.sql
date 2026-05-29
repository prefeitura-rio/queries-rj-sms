-- Critério 5 (intra-status) — solicitação SER travada no status "PENDENTE".
-- O próprio filtro de status desativa: quando o status muda, a linha some.
-- Folga: 10 dias. Emite a relação canônica bruta de 8 colunas.
{% set criterio_5_intervalo = 10 %}
{% set criterio_5_peso = monitora_cancer_pesos_clinicos()[4] %}

{{ monitora_cancer_criterio_intra_evento(
    criterio_label='SER_PENDENTE__STATUS_UPDATE',
    intervalo_urgencia=criterio_5_intervalo,
    peso=criterio_5_peso,
    source_filter="fonte = 'SER' and evento_status = 'PENDENTE' and data_solicitacao is not null",
    trigger_date_col='data_solicitacao',
    source_cte_name=ref('int_monitora_cancer__eventos_run_atual')
) }}
