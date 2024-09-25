
{% macro clean_abe_obs(text) %}
CASE 
    WHEN {{text}} = '' THEN null
    WHEN regexp_contains(lower(trim({{text}})),r'^((\.)|(none)|(baixa autom[á|a]tica)|(alta)|(alta ((com orientaç[õ|o]es)|(m[é|e]dica)|(administrativa)|(hospitalar)|(com prescr medica)))|(o+p)|(prescrevo( *e *oriento){0,1})|(prescriç[ã|a]o e orientaçao pa{0,1}ra casa)|(segue de alta com orientaç[o|õ]es gerais e sobre riscos e prescriç[a|ã]o)|((com ){0,1}orientaç[o|õ]es)|(orientaç[a|ã]o))$') THEN null
    ELSE {{text}}
END

{% endmacro %}