-- dias_atraso =
--   max( 0, date_diff(hoje, data_trigger) - intervalo_urgencia )

{% macro monitora_cancer_dias_atraso(data_inicio, intervalo_urgencia) %}
  GREATEST(
    0,
    DATE_DIFF(CURRENT_DATE('America/Sao_Paulo'), {{ data_inicio }}, DAY) - {{ intervalo_urgencia }}
  )
{% endmacro %}


/*
    Dias de atraso de um critério do score de gravidade do
    monitora_cancer.

    Conta os dias entre `data_inicio` (data do evento gatilho — pode ser
    data_trigger, data_solicitacao ou data_autorizacao, conforme o critério)
    e o dia de hoje no fuso America/Sao_Paulo, descontando o
    `intervalo_urgencia` (folga clinicamente tolerável em dias). O piso 0
    garante que, dentro da folga, dias_atraso = 0 — equivalente a "gatilho
    ativo mas dentro da janela tolerada".

    Parâmetros:
      • data_inicio: expressão SQL com a data do gatilho a partir da qual
        contar o atraso.
      • intervalo_urgencia: folga em dias subtraída do date_diff
        antes do GREATEST. Tipicamente a variável Jinja
        criterio_N_intervalo de int_monitora_cancer__gravidade_instancias.
        A cada intervalo_urgencia dias adicionais de atraso (depois da
        folga), o fator de tempo dias_atraso/intervalo_urgencia cresce em 1.
*/
