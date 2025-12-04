{% macro proper_estabelecimento(text) %}
    (
        select
            string_agg(
                case
                    when
                        lower(word) in (
                            'sms',
                            'ap',
                            'cap',
                            'caps',
                            'cer',
                            'cf',
                            'cms',
                            'cse',
                            'padi',
                            'smsdc',
                            'upa',
                            'a', 
                            'e',
                            'o',
                            'ii',
                            'iii',
                            'iv',
                            'vi',
                            'vii',
                            'viii',
                            'ix',
                            'xi',
                            'xii',
                            'xiii',
                            'xxiii',
                            'xiv',
                            'xv',
                            'xvi',
                            'xvii',
                            'xviii',
                            'xix'
                        )
                    then upper(word)
                    else {{ proper_br("word") }}
                end,
                ' '
            )
        from unnest(split({{ text }}, ' ')) as word
    )
{% endmacro %}
