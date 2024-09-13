
{% macro clean_cid(text) %}

    regexp_replace(
        regexp_replace(
            regexp_replace(
            {{text}},
            'Emissão de prescrição de repetição',
            ''
            ),
            '[E|e]xames* .*',
            'Exame'
        ),
        r' [\(|\[].*[\]|\)]',
        ''
    )

{% endmacro %}