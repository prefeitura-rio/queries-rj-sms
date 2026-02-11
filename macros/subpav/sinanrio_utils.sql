{% macro sinanrio_lista_cids_sintomaticos() %}
    ['Z030', 'Z111', 'Z201']
{% endmacro %}

{% macro sinanrio_padronize_sexo(coluna) %}
    case 
        when {{ remove_accents_upper(coluna) }} in ('M','MALE', 'MASCULINO', '1') then 1
        when {{ remove_accents_upper(coluna) }} in ('F','FEMALE', 'FEMININO', '2') then 2
        when {{ remove_accents_upper(coluna) }} in ('I','', 'INDEFINIDO', 'IGNORADO', 'IGN') then 3
        else null
    end
{% endmacro %}

{% macro sinanrio_padronize_raca_cor(coluna) %}
    case 
        when {{ remove_accents_upper(coluna) }} in (
            '1', 'BRANCA', 'BRANCO', 'B', 'BRNCA', 'BRNCO'
        ) then 1

        when {{ remove_accents_upper(coluna) }} in (
            '2', 'PRETA', 'PRETO', 'P', 'PRTA', 'PRTO'
        ) then 2

        when {{ remove_accents_upper(coluna) }} in (
            '3', 'PARDA', 'PARDO', 'PA', 'PRDA', 'PRDO'
        ) then 3

        when {{ remove_accents_upper(coluna) }} in (
            '4', 'AMARELA', 'AMARELO', 'A', 'AMRL'
        ) then 4

        when {{ remove_accents_upper(coluna) }} in (
            '5', 'INDIGENA', 'INDIGENO', 'INDIO', 'IND', 'IDG'
        ) then 5

        when {{ remove_accents_upper(coluna) }} in (
            'IGNORADA', 'IGNORADO'
        ) then 9

        else null
    end
{% endmacro %}