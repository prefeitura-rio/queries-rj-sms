-- dias_atraso =
-- dias_desde_trigger - threshold

{% macro monitora_cancer_dias_atraso(data_inicio, threshold) %}
  GREATEST(
    0,
    DATE_DIFF(CURRENT_DATE('America/Sao_Paulo'), {{ data_inicio }}, DAY) - {{ threshold }}
  )
{% endmacro %}


/*
    Dias de atraso de um subscore do monitora_cancer.

    Conta os dias entre `data_inicio` (data do evento gatilho — pode ser
    data_trigger, data_solicitacao ou data_autorizacao, conforme o subscore)
    e o dia de hoje no fuso America/Sao_Paulo, descontando o `threshold`
    (folga em dias antes de começar a contar atraso). O piso 0 garante que,
    dentro do threshold, dias_atraso = 0 — equivalente a "trigger ativo mas
    sem atraso ainda".

    Parâmetros:
      • data_inicio: expressão SQL com a data do gatilho a partir da qual
        contar o atraso.
      • threshold: dias de folga subtraídos do date_diff antes do GREATEST.
        Tipicamente a variável Jinja subscore_N_threshold do modelo.
*/
