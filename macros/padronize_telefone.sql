{% macro padronize_telefone(telefone_column) %}
    case
        when
            length(trim({{ telefone_column }})) = 0
            or trim({{ telefone_column }}) in ('NONE', 'NULL', '0', "()", "")
            or regexp_contains(trim({{ telefone_column }}), r'E\+\d+')  -- Remove scientific notation
            or regexp_contains(trim({{ telefone_column }}), r'[a-zA-Z]')  -- Remove numbers that contain letters
            or length(safe.regexp_replace(trim( {{ telefone_column }} ), substr({{ telefone_column }}, 1, 1), '')) = 0 -- Remove repeated digits
        then null
        else
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            trim({{ telefone_column }}), '^0', ''  -- Remove leading 0
                        ),
                        '[()]',
                        ''  -- Remove parentheses
                    ),
                    '-',
                    ''  -- Remove hyphens
                ),
                ' ',  -- Remove blank spaces
                ''
            )
    end
{% endmacro %}
