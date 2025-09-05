{% macro proper_br(text) %}
    (
        select
            string_agg(
                case
                    when
                        lower(word) in (
                            -- artigos
                            'um', 'uma',
                            'uns', 'umas',
                            'o', 'a',
                            'os', 'as',
                            -- a
                            'à', 'ao',
                            'às', 'aos',
                            -- de
                            'de',
                            'da', 'das',
                            'do', 'dos',
                            -- em
                            'em',
                            'na', 'no',
                            'nas', 'nos',
                            -- ...
                            'por',
                            'para',
                            'com',
                            'e',
                            'é'
                        )
                    then lower(word)
                    when
                        lower(word) in (
                            'i', 'ii', 'iii', 'iv', 'v',
                            'vi', 'vii', 'viii', 'ix', 'x'
                        )
                    then upper(word)
                    else concat(upper(substr(word, 1, 1)), lower(substr(word, 2)))
                end,
                ' '
            )
        from unnest(
            split(
                {{ 
                    aux_remove_suffix(
                        aux_remove_prefix(
                            remove_duplicate_whitespace(text)
                        )
                    )
                }},
                ' '
            )
        ) as word
    )
{% endmacro %}
