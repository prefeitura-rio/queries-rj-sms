{% macro padronize_cep(cep_column) %}

    case
        -- Caso tenha 13 dígitos, remover os 5 primeiros se os 5 posteriores forem
        -- iguais
        when
            length({{ cep_column }}) = 13
            and substr(trim(replace({{ cep_column }}, '-', '')), 1, 5)
            = substr(trim(replace({{ cep_column }}, '-', '')), 6, 5)
        then substr(trim(replace({{ cep_column }}, '-', '')), 6, 13)
        when
            length({{ cep_column }}) = 13
            and substr(trim(replace({{ cep_column }}, '-', '')), 1, 5)
            != substr(trim(replace({{ cep_column }}, '-', '')), 6, 5)
        then null

        -- Caso número de dígitos seja 6 ou 7, fixar os 5 primeiros e preencher com 0
        -- à esquerda até ter 8 dígitos
        when length({{ cep_column }}) in (6, 7)
        then
            concat(
                substr(trim(replace({{ cep_column }}, '-', '')), 1, 5),
                lpad(substr(trim(replace({{ cep_column }}, '-', '')), 6, 2), 3, '0')
            )

        -- Caso tenha 5 ou menos dígitos, preencher com 0 à direita até ter 8 dígitos
        when length({{ cep_column }}) <= 5
        then rpad(trim(replace({{ cep_column }}, '-', '')), 8, '0')

        -- Caso não se encaixe em nenhuma das condições, manter o cep_column original
        else trim(replace({{ cep_column }}, '-', ''))
    end
{% endmacro %}
