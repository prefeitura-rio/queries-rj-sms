{% macro remove_decode_chars_error(text) %}

    {# Sinta-se à vontade para contribuir com esta macro :D (Em especial você, Avellar)#}
    {%  set decode_chars =  {
        'Ã€':'À',   'Ã‚':'Â',   'Ãƒ':'Ã',   'Ã…':'Å',   'Ã†':'Æ',
        'Ã‡':'Ç',   'Ã‰':'É',   'ÃŠ':'Ê',   'ÃŒ':'Ì',   'ÃŽ':'Î',   
        'Ã§':'ç',   'Ã£':'ã',   'Ã¡':'á',   'Ã¢':'â',   'Ã¤':'ä',   
        'Ã¥':'å',   'Ã¦':'æ',   'Ã¨':'è',   'Ã©':'é',   'Ãª':'ê',   
        'Ã«':'ë',   'Ã¬':'ì',   'Ã®':'î',   'Ã¯':'ï',   'Ã²':'ò',   
        'Ã³':'ó',   'Ã´':'ô',   'Ãµ':'õ',   'Ã¶':'ö',   'Ã·':'÷',   
        'Ã¹':'ù',   'Ãº':'ú',   'Ã»':'û',   'Ã¼':'ü'
        }  
    %}

    {% set ns = namespace(sql_expr=text) %}

    {% for decoded_char, char in decode_chars.items() %}
        {% set ns.sql_expr = "replace(" ~ ns.sql_expr ~ ", '" ~ decoded_char ~ "', '" ~ char ~ "')" %}
    {% endfor %}

    {{ns.sql_expr}}
{% endmacro %}