{% macro process_null_sih(texto) %}
    nullif(
        nullif(
            nullif(
                nullif(
                    nullif(
                        nullif(
                            nullif(
                                nullif(
                                    nullif(
                                        nullif(
                                            nullif(
                                                nullif({{ texto }}, 'null'),
                                                'None'
                                            ),
                                            '    '
                                        ),
                                        '00000000000' 
                                    ),
                                    '0000000000000'
                                ),
                                '00000000000'
                            ),
                            '0000'
                        ),
                        '000000000000' 
                    ),
                    '000000000000000' 
                ),
                '000000'
            ),
            '0'
        ),
        'NA'
    )
{% endmacro %}
