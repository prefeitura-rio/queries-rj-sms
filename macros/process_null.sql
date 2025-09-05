{% macro process_null(texto) %}
    if(
        lower(
            trim({{ texto }})
        ) in (
            '',
            '-',
            'null',
            'none',
            'n/a',
            'na',
            'nan',
            'nat'
        ),
        null,
        {{ texto }}
    )
{% endmacro %}
