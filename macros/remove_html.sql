{% macro remove_html(column_name) %}

    {# Dicionário de Entidades HTML #}
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
        
        '&ccedil;': 'ç',    '&Ccedil;': 'Ç',    '&ordf;': 'ª',      '&ordm;': 'º'
    } %}

    {% set ns = namespace(sql_expr=column_name) %}

    {% for entity, char in entities.items() %}
        {% set ns.sql_expr = "replace(" ~ ns.sql_expr ~ ", '" ~ entity ~ "', '" ~ char ~ "')" %}
    {% endfor %}

    trim(regexp_replace(
        {{ ns.sql_expr }},
        '<[^>]+>', ' '
    ))

{% endmacro %}