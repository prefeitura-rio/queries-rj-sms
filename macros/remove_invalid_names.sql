{% macro remove_invalid_names(texto) %}
        WHEN {{ texto }} IN ("NONE") THEN NULL
        WHEN REGEXP_CONTAINS({{ texto }}, r'^(X+)$') THEN NULL   -- Remove valores com letras repeditas
        WHEN REGEXP_CONTAINS({{ texto }}, r'^[A-Za-z]$') THEN NULL  -- Remove valores com apenas uma letra
        WHEN {{ texto }} IN (
            'DESCONHECIDO', 'FORA DO TERRITORIO', 'MUDOU SE', 'N', 'NAO', 
            'NAO CONSTA', 'NAO DECLARADA', 'NAO DECLARADO', 'NAO IDENTIFICADO', 
            'NAO INF', 'NAO INFORMADO', 'NAO POSSUI', 'NAO TEM', 'ND', 'NI', 
            'NR', 'PLANO EMPRESA', 'PLANO INDIVIDUAL', 'SEM', 'SEM INF', 
            'SEM INFO', 'SEM INFOR', 'SEM INFORMAAAO', 'SEM INFORMACAO', 
            'SI', 'SN', 'TESTE'
        ) THEN NULL
{% endmacro %}
