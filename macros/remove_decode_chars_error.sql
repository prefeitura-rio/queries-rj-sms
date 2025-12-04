{% macro remove_decode_chars_error(text) %}
    {# Sinta-se à vontade para contribuir com esta macro :D (Em especial você, Avellar)#}
    {# avellar esteve aki  ´(· ᴥ ·^ )/ #}

    {% set ns = namespace(sql_expr=text) %}

    {# [Ref] https://www.i18nqa.com/debug/utf8-debug.html #}
    {% set decode_chars = {
        'â‚¬':'€',
        'â€¦':'…', 'â€¢':'•', 'â€“':'–', 'â€”':'—', 'â„¢':'™',
        'Å’':'Œ', 'Å½':'Ž', 'Å¡':'š', 'Å“':'œ', 'Å¾':'ž', 'Å¸':'Ÿ',

        'Â¡':'¡', 'Â¢':'¢', 'Â£':'£', 'Â¤':'¤', 'Â¥':'¥',
        'Â¦':'¦', 'Â§':'§', 'Â¨':'¨', 'Â©':'©', 'Âª':'ª',
        'Â«':'«', 'Â¬':'¬', 'Â®':'®', 'Â¯':'¯', 'Â°':'°',
        'Â±':'±', 'Â²':'²', 'Â³':'³', 'Â´':'´', 'Âµ':'µ',
        'Â¶':'¶', 'Â·':'·', 'Â¸':'¸', 'Â¹':'¹', 'Âº':'º',
        'Â»':'»', 'Â¼':'¼', 'Â½':'½', 'Â¾':'¾', 'Â¿':'¿',
        'Å':'Š',

        'Ã€':'À',           'Ã‚':'Â', 'Ãƒ':'Ã', 'Ã„':'Ä', 'Ã…':'Å',
        'Ã†':'Æ', 'Ã‡':'Ç',
        'Ãˆ':'È', 'Ã‰':'É', 'ÃŠ':'Ê', 'Ã‹':'Ë',
        'ÃŒ':'Ì', 'ÃŽ':'Î',         
                  'Ã‘':'Ñ',
        'Ã’':'Ò', 'Ã“':'Ó', 'Ã”':'Ô', 'Ã•':'Õ', 'Ã–':'Ö',
        'Ã—':'×', 'Ã˜':'Ø',
        'Ã™':'Ù', 'Ãš':'Ú', 'Ã›':'Û', 'Ãœ':'Ü',
                  'Ãž':'Þ', 'ÃŸ':'ß',
                  'Ã¡':'á', 'Ã¢':'â', 'Ã£':'ã', 'Ã¤':'ä', 'Ã¥':'å',
        'Ã¦':'æ', 'Ã§':'ç',
        'Ã¨':'è', 'Ã©':'é', 'Ãª':'ê', 'Ã«':'ë',
        'Ã¬':'ì', 'Ã®':'î', 'Ã¯':'ï',
        'Ã°':'ð', 'Ã±':'ñ',
        'Ã²':'ò', 'Ã³':'ó', 'Ã´':'ô', 'Ãµ':'õ', 'Ã¶':'ö',
        'Ã·':'÷', 'Ã¸':'ø',
        'Ã¹':'ù', 'Ãº':'ú', 'Ã»':'û', 'Ã¼':'ü',
        'Ã½':'ý', 'Ã¾':'þ', 'Ã¿':'ÿ'
    } %}
    {% for decoded_char, char in decode_chars.items() %}
        {% set ns.sql_expr = "replace(" ~ ns.sql_expr ~ ", '" ~ decoded_char ~ "', '" ~ char ~ "')" %}
    {% endfor %}

    {#
        0xC3 0x83 (U+00C3 Ã)          >  0xC3 0x81 (U+00C1 Á)
        0xC2 0x81 (U+0081 <control>) /

        0xC3 0x83 (U+00C3 Ã)          >  0xC3 0x8D (U+00CD Í)
        0xC2 0x8D (U+008D REVERSE LINE FEED)

        0xC3 0x83 (U+00C3 Ã)          >  0xC3 0x8F (U+00CF Ï)
        0xC2 0x8F (U+008F SINGLE SHIFT THREE)
        0xC3 0x83 (U+00C3 Ã)          >  0xC3 0x90 (U+00D0 Ð)
        0xC2 0x90 (U+0090 DEVICE CONTROL STRING)

        0xC3 0x83 (U+00C3 Ã)          >  0xC3 0x9D (U+00DD Ý)
        0xC2 0x9D (U+009D OPERATING SYSTEM COMMAND)

        0xC3 0x83 (U+00C3 Ã)          >  0xC3 0xA0 (U+00E0 à)
        0xC2 0xA0 (U+00A0 NBSP)      /

        0xC3 0x83 (U+00C3 Ã)          >  0xC3 0xAD (U+00ED í)
        0xC2 0xAD (U+00AD SOFT HYPHEN)
    #}
    {% set decode_bytes =  {
        'c383c281': 'Á',
        'c383c28d': 'Í',
        'c383c28f': 'Ï',
        'c383c290': 'Ð',
        'c383c29d': 'Ý',
        'c383c2a0': 'à',
        'c383c2ad': 'í'
    } %}
    {% for from, to in decode_bytes.items() %}
        {% set ns.sql_expr =
            "replace(" ~ ns.sql_expr ~ ", "
                ~ "cast(from_hex('" ~ from ~ "') as string), "
                ~ "'" ~ to ~ "'"
            ~ ")"
        %}
    {% endfor %}

    {{ ns.sql_expr }}
{% endmacro %}