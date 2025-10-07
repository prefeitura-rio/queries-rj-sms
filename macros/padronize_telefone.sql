{% macro padronize_telefone(telefone_column) %}
case
    -- Remove nulos comuns: 'null', 'none', etc
    when {{ process_null(telefone_column) }} is null
        then null

    when
        regexp_replace({{ telefone_column }}, r'[^0-9]', '') like "0000%"  -- Remove números iniciando com 4 zeros
        or regexp_contains(
            regexp_replace({{ telefone_column }}, r'[^0-9]', ''),
            r"^0*9*0*$"   -- Números somente com 9's seguidos de 0's
        )
        -- Remove (alguns) placeholders
        or regexp_replace({{ telefone_column }}, r'[^0-9]', '') in (
            "999999998",
            "998989898",
            "989898989",
            "989999999",
            "988888888",
            "980000000",
            "966666666",
            "32323232",
            "22222222",
            "21212121",
            "21000000",
            "20000000"
        )
        or regexp_contains({{ telefone_column }}, r'[a-zA-Z]')  -- Remove strings contendo letras
        or length(
            safe.regexp_replace(
                regexp_replace({{ telefone_column }}, r'[^0-9]', ''),
                substr({{ telefone_column }}, 1, 1),
                ''
            )
        ) = 0  -- Remove strings com 1 caractere repetido
        then null

    when
        -- Confere se o número pós tratamento final tem tamanho <8
        length(
            regexp_replace(
                regexp_replace(
                    {{ telefone_column }},
                    r'[^0-9]', -- Remove tudo que não seja dígito
                    ''
                ),
                r'^0+', -- Remove 0s no início
                ''
            )
        ) < 8
        then null

    when
        -- Caso o número possua prefixo +55 (código do Brasil), remove
        -- ex.: "+55 (21) 8765-4321" (tam. 12); "+55 (21) 98765-4321" (tam. 13)
        length(
            regexp_replace(regexp_replace({{ telefone_column }}, r'[^0-9]', ''), r'^0+', '')
        ) in (12, 13)
        then
            regexp_replace(
                regexp_replace(
                    {{ telefone_column }},
                    r'[^0-9]', ''),
                r'^0*55',
                ''
            )
    else regexp_replace(regexp_replace({{ telefone_column }}, r'[^0-9]', ''), r'^0+', '')
end
{% endmacro %}
