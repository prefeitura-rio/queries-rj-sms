{% macro monitora_cancer_criterio_intra_evento(
    criterio_label,
    intervalo_urgencia,
    peso,
    source_filter,
    trigger_date_col,
    etapa=none,
    source_cte_name='eventos_run_atual'
) %}
        select
            cpf_particao,
            '{{ criterio_label }}' as criterio,
            {% if etapa is not none %}'{{ etapa }}'{% else %}cast(null as string){% endif %} as etapa,
            {{ trigger_date_col }} as data_trigger,
            {{ monitora_cancer_dias_atraso(trigger_date_col, intervalo_urgencia) }} as dias_atraso,
            {{ intervalo_urgencia }} as intervalo_urgencia_dias,
            risco as risco_evento_gatilho,
            {{ peso }} as peso_criterio
        from {{ source_cte_name }}
        where {{ source_filter }}
{% endmacro %}


/*
    Instância bruta de um critério INTRA-EVENTO do score de gravidade do
    monitora_cancer (critérios 4, 5, 6).

    Emite a relação canônica bruta de 8 colunas (mesma do
    monitora_cancer_criterio_cross_evento; ANTES da fórmula Eq. 1, aplicada
    depois do UNION no select final de int_monitora_cancer__gravidade_instancias):
      cpf_particao, criterio, etapa, data_trigger, dias_atraso,
      intervalo_urgencia_dias, risco_evento_gatilho, peso_criterio.

    Sem anti-join: a desativação está no próprio source_filter — quando a
    condição deixa de valer (data do desfecho preenchida, ou status muda),
    a linha some do output. Cobre dois padrões:
      • intra-evento por progresso de data (critério 4, 2 legs): o filtro
        inclui `<data_proxima> IS NULL`.
      • intra-status "stuck" (critérios 5, 6): o filtro inclui
        `evento_status = '<STATUS>'`.

    Parâmetros:
      • criterio_label     — string; valor da coluna `criterio`.
      • intervalo_urgencia — int; folga clínica (divisor do fator de tempo
                             e valor de intervalo_urgencia_dias). Tipicamente
                             a variável Jinja criterio_N_intervalo.
      • peso               — float; peso clínico (criterio_N_peso, lido de
                             monitora_cancer_pesos_clinicos).
      • source_filter      — string SQL; cláusula WHERE arbitrária. DEVE
                             garantir a desativação (ex.: `data_autorizacao
                             IS NULL`, `evento_status = 'PENDENTE'`).
      • trigger_date_col   — string; coluna usada como data_trigger e base
                             do dias_atraso (`data_solicitacao` ou
                             `data_autorizacao`).
      • etapa              — string|none; default none ⇒ etapa NULL. Só o
                             critério 4 preenche (SOLICITACAO_AUTORIZACAO /
                             AUTORIZACAO_EXECUCAO), para distinguir os legs.
      • source_cte_name    — string; default 'eventos_run_atual'.

    Lê de uma CTE local (não ref()). dias_atraso via
    monitora_cancer_dias_atraso (piso 0).
*/
