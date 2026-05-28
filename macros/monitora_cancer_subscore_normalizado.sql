-- subscore_normalizado =
-- subscore / ( max_risco_trigger * 2^(teto_atraso / dias_dobrar_subscore) )

{% macro monitora_cancer_subscore_normalizado(
    subscore_valor,
    dias_dobrar_subscore,
    teto_atraso,
    max_risco_trigger=4
) %}
    {{ subscore_valor }} / (
      {{ max_risco_trigger }} * POW(
        2,
        {{ teto_atraso }} / {{ dias_dobrar_subscore }}
        )
    )
{% endmacro %}


/*
    Normaliza um subscore de gravidade do monitora_cancer para [0, 1].

    Divide o valor bruto do subscore (produzido por
    [[monitora_cancer_subscore]]) pelo MAIOR valor que ele poderia assumir,
    dado o teto de atraso e a constante de risco máximo do projeto:

    O denominador é o valor que o próprio subscore atingiria com
    risco_trigger no máximo da escala (max_risco_trigger) e dias_atraso
    saturado no teto (teto_atraso). Como o subscore bruto é monotonicamente
    crescente em dias_atraso e em risco_trigger, e ambos têm cotas
    superiores, o resultado fica no intervalo (0, 1].

    Parâmetros:
      • subscore_valor: expressão SQL com o subscore bruto a normalizar
        (saída de [[monitora_cancer_subscore]]).
      • dias_dobrar_subscore: mesmo parâmetro usado para produzir o
        subscore bruto — controla quão rápido o termo exponencial cresce.
      • teto_atraso: cap de dias_atraso usado no subscore bruto (LEAST).
        Precisa ser o MESMO valor passado para [[monitora_cancer_subscore]],
        senão a normalização vira inconsistente com o numerador.
      • max_risco_trigger: maior valor possível de risco_trigger no
        domínio do projeto. Default 4 (escala 1..4 em monitora_cancer).
*/