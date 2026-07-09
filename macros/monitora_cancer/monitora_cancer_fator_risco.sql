-- fator_risco =
--   ( COALESCE(risco, risco_padrao_quando_nulo) + amortecedor_risco )
--   / ( risco_maximo_escala + amortecedor_risco )

{% macro monitora_cancer_fator_risco(
    risco_evento_gatilho,
    amortecedor_risco,
    risco_maximo_escala,
    risco_padrao_quando_nulo=2
) %}
  (
    (COALESCE({{ risco_evento_gatilho }}, {{ risco_padrao_quando_nulo }}) + {{ amortecedor_risco }})
    / ({{ risco_maximo_escala }} + {{ amortecedor_risco }})
  )
{% endmacro %}

/*
    Fator de risco amortecido — componente do score de gravidade do
    monitora_cancer.

    "Puxa para o meio" o risco bruto (1..4) na escala normalizada: o
    amortecedor_risco controla quanto o risco bruto influencia o score
    final. Reflete a incerteza de classificação nos sistemas externos
    (SISREG, SER, SISCAN): risco máximo não vale 4× risco mínimo, vale
    (risco_maximo_escala + amortecedor_risco) / (1 + amortecedor_risco) ×
    o risco mínimo.

    NULL é tratado como `risco_padrao_quando_nulo` (default 2 — mediana
    da escala 1..4), em vez de assumir o caso mais leve (1).

    FONTE ÚNICA — consumido por:
      • int_monitora_cancer__gravidade_instancias
            → coluna fator_risco
      • macros/monitora_cancer_gravidade_criterio
            → componente da fórmula gravidade_criterio

    Referência metodológica: OECD/JRC Handbook on Constructing Composite
    Indicators (Nardo et al., 2008), normalização "distance to reference"
    (§5.4) com fator de amortecimento aditivo.
*/
