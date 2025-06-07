{% macro process_null(texto) %}
    nullif(
        nullif(
            nullif(
                nullif(
                    nullif(
                        nullif(
                            nullif(
                                nullif(
                                    nullif(
                                        nullif(nullif(nullif({{ texto }}, 'null'), 'None'), ''),
                                        'NaT'
                                    ),
                                    'nan'
                                ),
                                'Null'
                            ),
                            'NULL'
                        ),
                        'NONE'
                    ),
                    'none'
                ),
                'nat'
            ),
            'NA'
        ),
        'na'
    )
{% endmacro %}
