{% macro remove_html(text) %}

    {% set ns = namespace(sql_expr=text) %}

    -- tags simples
    {% set tags = {
        '<p>':'',       '</p>':'',
        '<div>':'',     '</div>':'',
        '<h3>':'',      '</h3>':'\\n',
        '<b>':'',       '</b>':'',
        '<strong>':'',  '</strong>':'',
        '<br>':'\\n',   '<br />':'\\n',
        '<em>':'',      '</em>':'',
        '<tr>':'',      '</tr>':'',
        '<u>':'',       '</u>':'',
        '<td>':'',       '</td>':'',
        '<tbody>':'',    '</tbody>':'',
        '<table>':'',    '</table>':'',
        '<li>':'- ',    '</li>':'\\n',
        '<ul>':'\\n',   '</ul>':'\\n',
        '<ol>':'\\n',   '</ol>':'\\n',

    } %}

    {% for tag, value in tags.items() %}
        {% set ns.sql_expr = "replace(" ~ ns.sql_expr ~ ", '" ~ tag ~ "', '" ~ value ~ "')" %}
    {% endfor %}

    -- tags com atributos
    {% set patterns = {
        '<a\s+[^>]*>': '', '</a>': '',
        '<div\s+[^>]*>': '', '</div>': '',
        '<p\s+align="[^"]*">': '',
        '<img\s+[^>]*>': '',
        '</?(table|tbody|tr|td)\b[^>]*>': '',
    } %}

    {% for pattern, replacement in patterns.items() %}
        {% set ns.sql_expr = "regexp_replace(" ~ ns.sql_expr ~ ", r'" ~ pattern ~ "', '" ~ replacement ~ "')" %}
    {% endfor %}

    -- entidades HTML
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
        '&rdquo;': '”',     '&ldquo;':'“',      '&deg;':'°'
    } %}

    {% for entity, char in entities.items() %}
        {% set ns.sql_expr = "replace(" ~ ns.sql_expr ~ ", '" ~ entity ~ "', '" ~ char ~ "')" %}
    {% endfor %}

    trim({{ ns.sql_expr }})

{% endmacro %}