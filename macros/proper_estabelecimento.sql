{% macro proper_estabelecimento(text) %}
    (
        select
            string_agg(
                case
                    when
                        lower(word) in (
                            'sms',   -- Secretaria Municipal de Saúde
                            'ses',   -- Secretaria de Estado de Saúde
                            'ms',    -- Ministério da Saúde
                            'smsdc',
                            'ap',    -- Área Programática
                            'cap',
                            'caps',  -- Centro de Atenção Psicossocial
                            'cer',   -- Centro de Emergência Regional
                            'cf',    -- Clínica da Família
                            'cms',   -- Centro Municipal de Saúde
                            'cse',   -- Centro de Saúde Escola
                            'padi',  -- Programa de Atenção Domiciliar ao Idoso
                            'upa',   -- Unidade de Pronto Atendimento
                            'par',   -- Ponto de Apoio na Rua
                            'ad',    -- (CAPS) Álcool e Drogas
                            'eat',   -- (CAPS) Espaço Aberto ao Tempo
                            'uerj',
                            'ufrj',
                            'rj',

                            'i',  'ii',  'iii',  'iv',  'v',  'vi',  'vii',  'viii',  'ix', 'x',
                            'xi', 'xii', 'xiii', 'xiv', 'xv', 'xvi', 'xvii', 'xviii', 'xix', 'xx',
                            'xxiii'
                        )
                        then upper(word)
                    when lower(word) = 'capsi' -- Condição específica para retornar "CAPSi"
                        then 'CAPSi'
                    when lower(word) = "capsiii"  -- Typo "Capsiii Clarice Lispector"
                        then "CAPS III"
                    else {{ proper_br("word") }}
                end,
                ' '
            )
        from unnest(split({{ text }}, ' ')) as word
    )
{% endmacro %}