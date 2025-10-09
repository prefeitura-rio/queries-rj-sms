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
                            'xix',
                            'ad',
                            'eat'
                        )
                    then upper(word)
                    when lower(word) = 'capsi' -- Condição específica para retornar "CAPSi"
                    then 'CAPSi'
                    else {{ proper_br("word") }}
                end,
                ' '
            )
        from unnest(split({{ text }}, ' ')) as word
    )
{% endmacro %}