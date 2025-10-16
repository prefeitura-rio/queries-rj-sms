{% macro remove_invalid_email(texto) %}
case
    -- Remove nulos comuns: 'null', 'none', etc
    when {{ process_null(telefone_column) }} is null
        then null

    -- Remove valores com um único caractere repetido
    -- vide macro remove_invalid_names()
    when regexp_contains(
        trim(split({{ texto }}, '@')[offset(0)]),
        r'(?i)^(A+|B+|C+|D+|E+|F+|G+|H+|I+|J+|K+|L+|M+|N+|O+|P+|Q+|R+|S+|T+|U+|V+|W+|X+|Y+|Z+|0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$'
    )
        and trim(upper(split({{ texto }}, '@')[offset(1)])) in ("GMAIL.COM", "HOTMAIL.COM")
        then null

    -- Remove valores com até 2 caracteres em servidores públicos
    when length(trim(split({{ texto }}, '@')[offset(0)])) <= 2
        and trim(upper(split({{ texto }}, '@')[offset(1)])) in ("GMAIL.COM", "HOTMAIL.COM")
        then null
    
    when trim(upper(split({{ texto }}, '@')[offset(1)])) in (
        "N.COM",
        "NAO.COM",
        "NAO.COM.BR",
        "SEMEMAIL.COM",

        "TEM.COM",
        "NAOTE.COM",
        "NAOTEM.COM",
        "NAOTEM.COM.BR",
        "NAOTEMEMAIL.COM",
        "NAOTEMEMAIL.COM.BR",
        "NAOTEM.GMAIL.COM",
        "NAOTEMIMAIL.COM.BR",
        "NAOTEMIMAIL.GMAIL.COM.BR",
        "O.TEM",
        "OTEM.COM",
        "NAOTEMCADASTRO.COM",

        "POSSUI.COM",
        "NAOPOSSUI.COM",
        "NAOPOSSUI.COM.BR",
        "NAOPOSSUIEMAIL.COM",

        "INFORMOU.COM",
        "INFORMADO.COM",
        "NAOINFORMADO.COM",
        "NAOINFORMADO.COM.BR",
        "NAOTEMINFORMACAO.COM.BR",

        "SEMINFO.COM.BR",
        "SEMINFORMACAO.COM.BR",
        "SEMINFORMACOES.COM.BR",
        "OINFORMOU.COM",

        "NAOEXISTE.COM",
        "INEXISTENTE.COM",

        "NAOSABE.COM",
        "ND.COM.BR",
        "EMAIL.COM.BR",
        "123.COM",
        "XX.XX",
        "XXX.XX",
        "XXX.COM"
        "XXXX.COM",
        "XXXXX.COM"
    )
        then null

    when upper(
        regexp_replace(
            normalize(
                split({{ texto }}, '@')[offset(0)],
                NFD  -- Remove acentos, marcas
            ),
            r'[^\p{Letter}0-9]', -- Substitui tudo que não for letra ou dígito
            ''                   -- por nada
        )
    ) in (
        "NT",
        "NTEM",
        "NAOTEM",
        "NAOTEMNAOTEM",
        "NAOTEMEMAIL",
        "NAOTEMIMAIL",
        "NAOTENHO",
        "NOTEM",
        "NOATEM",
        "NAOTEM23",
        "NAOTE",

        "NP",
        "NPOSSUI",
        "NAOPOSSUI",
        "NAOPOSSUO",
        "NAOPOSSUE",
        "NAOPOSSUIEMAIL",
        "NAOPOSSUIIMAIL",
        "NAOPOSSUICFZA",

        "NAOINFORMOU",
        "NAOINFORMADO",
        "NAOINFROMADO",
        "NINFORMADO",
        "NAOINFO",
        "NAOINF",
        "EMAILNAOINFORMADO",
        "NAOINFORMA",

        "SEM",
        "SEMINF",
        "SEMINFO",
        "SEMINFORMACAO",
        "SEMINFORMACOES",
        "SEMINFOR",

        "ND",
        "NAODECLARADO",

        "NAO",
        "NAONAO",
        "NENHUM",
        "NAOUSA",
        "NOT",
        "NOEMAIL",
        "NAOSEAPLICA",
        "SEMEMAIL",

        "NAOSEI",
        "IGNORADO",
        "NAOCONSTA",

        "ESTOUSEMEMAILNOMOMENTO",

        "123",
        "123ABC",
        "EU",
        "CF",
        "EMAIL",
        "GMAIL",
        "TESTE",
        "TESTETESTE",
        "XXXXX",
        "XXXXXX"
    )
        then null

    else trim(lower({{ text }}))
end
{% endmacro %}
