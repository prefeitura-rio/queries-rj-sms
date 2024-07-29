{% macro proper_br(text) %}
    (
        select
            string_agg(
                case
                    when
                        lower(word) in (
                            'a',
                            'à',
                            'ao',
                            'com',
                            'e',
                            'é',
                            'em',
                            'da',
                            'das',
                            'de',
                            'do',
                            'dos',
                            'na',
                            'no',
                            'o',
                            'para',
                            'por',
                            'um'
                        )
                    then lower(word)
                    else concat(upper(substr(word, 1, 1)), lower(substr(word, 2)))
                end,
                ' '
            )
        from unnest(split({{ text }}, ' ')) as word
    )
{% endmacro %}
