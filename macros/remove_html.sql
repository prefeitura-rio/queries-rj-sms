{% macro remove_html(text) %}

    {% set ns = namespace(sql_expr=text) %}

    {% set tags = {
        '<p>':'',       '</p>':'',
        '<h3>':'',      '</h3>':'\\n',
        '<b>':'',       '</b>':'',
        '<strong>':'',  '</strong>':'',
        '<br>':'\\n',    '<br />':'\\n'
    } %}

    {% for tag, value in tags.items() %}
        {% set ns.sql_expr = "replace(" ~ ns.sql_expr ~ ", '" ~ tag ~ "', '" ~ value ~ "')" %}
    {% endfor %}


    {% set entities = {
        '&nbsp;': ' ',      '&amp;': '&',       '&quot;': '"',
        '&lt;': '<',        '&gt;': '>',        '&ndash;': '-',     '&mdash;': '-',
        
        '&aacute;': 'á',    '&Aacute;': 'Á',    '&agrave;': 'à',    '&Agrave;': 'À',
        '&acirc;': 'â',     '&Acirc;': 'Â',     '&atilde;': 'ã',    '&Atilde;': 'Ã',
        '&auml;': 'ä',      '&Auml;': 'Ä',
        
        '&eacute;': 'é',    '&Eacute;': 'É',    '&egrave;': 'è',    '&Egrave;': 'È',
        '&ecirc;': 'ê',     '&Ecirc;': 'Ê',     '&euml;': 'ë',      '&Euml;': 'Ë',
        
        '&iacute;': 'í',    '&Iacute;': 'Í',    '&igrave;': 'ì',    '&Igrave;': 'Ì',
        '&icirc;': 'î',     '&Icirc;': 'Î',     '&iuml;': 'ï',      '&Iuml;': 'Ï',
        
        '&oacute;': 'ó',    '&Oacute;': 'Ó',    '&ograve;': 'ò',    '&Ograve;': 'Ò',
        '&ocirc;': 'ô',     '&Ocirc;': 'Ô',     '&otilde;': 'õ',    '&Otilde;': 'Õ',
        '&ouml;': 'ö',      '&Ouml;': 'Ö',
        
        '&uacute;': 'ú',    '&Uacute;': 'Ú',    '&ugrave;': 'ù',    '&Ugrave;': 'Ù',
        '&ucirc;': 'û',     '&Ucirc;': 'Û',     '&uuml;': 'ü',      '&Uuml;': 'Ü',
        
        '&ccedil;': 'ç',    '&Ccedil;': 'Ç',    '&ordf;': 'ª',      '&ordm;': 'º',
        '&rdquo;': '”',     '&ldquo;':'“'
    } %}

    {% for entity, char in entities.items() %}
        {% set ns.sql_expr = "replace(" ~ ns.sql_expr ~ ", '" ~ entity ~ "', '" ~ char ~ "')" %}
    {% endfor %}

    trim({{ ns.sql_expr }})

{% endmacro %}