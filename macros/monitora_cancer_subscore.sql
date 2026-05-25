-- subscore_valor =
-- fator_risco * 2^(dias_atraso / dias_dobrar_subscore)

{% macro monitora_cancer_subscore(dias_atraso, dias_dobrar_subscore, risco_trigger, teto_atraso) %}
  COALESCE({{ risco_trigger }}, 1) * POW(
    2,
    LEAST({{ dias_atraso }}, {{ teto_atraso }}) / {{ dias_dobrar_subscore }}
  )
{% endmacro %}

/*
    Valor de um subscore de gravidade do monitora_cancer.

    Um subscore é caracterizado pela tupla
    (trigger, expected, threshold, dias_dobrar_subscore, risco_trigger).
    O par (trigger, expected, threshold) entra no cálculo apenas de
    `dias_atraso` (feito fora desta macro). O trigger só gera subscore
    quando o expected ainda NÃO aconteceu — eventos cujo expected já chegou
    são filtrados antes de chamar a macro. Esta macro combina dias_atraso,
    dias_dobrar_subscore e risco_trigger no valor final do subscore:

        subscore_valor = COALESCE(risco_trigger, 1)
                       * 2 ^ ( LEAST(dias_atraso, teto_atraso) / dias_dobrar_subscore )        

    Componentes:
      • Fator de risco: COALESCE(risco_trigger, 1). risco_trigger é o risco
        do evento gatilho (1..4, ou NULL quando não mapeado). Multiplica
        diretamente o subscore. NULL → fator 1 (sem agravador).
      • Termo exponencial: a cada `dias_dobrar_subscore` dias de atraso, o
        termo dobra. O teto `teto_atraso` evita explosão exponencial — com
        base 2 e dias_dobrar = 5, 90 dias já correspondem a 2^18 = 262144.

    Retorna FLOAT64 (POW e a divisão INT/INT em BigQuery resultam em FLOAT64).
*/