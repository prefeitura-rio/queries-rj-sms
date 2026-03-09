{% macro padroniza_telefone_whatsapp(telefone_column, total_cpfs_column='0', flag_telefone_clinica_column='false') %}

    {%- set telefone_str = "trim(cast(" ~ telefone_column ~ " as string))" -%}
    {%- set telefone_digits = "regexp_replace(" ~ telefone_str ~ ", r'[^0-9]', '')" -%}
    {%- set telefone_limpo -%}
        case
            when regexp_contains({{ telefone_digits }}, r'^0\d{11}$')
                then substr({{ telefone_digits }}, 2)
            else {{ telefone_digits }}
        end
    {%- endset -%}

    {%- set ddd_regex -%}
        r'^(11|12|13|14|15|16|17|18|19|21|22|24|27|28|31|32|33|34|35|37|38|41|42|43|44|45|46|47|48|49|51|53|54|55|61|62|63|64|65|66|67|68|69|71|73|74|75|77|79|81|82|83|84|85|86|87|88|89|91|92|93|94|95|96|97|98|99)$'
    {%- endset -%}

    {%- set ddd_valido -%}
        (
            regexp_contains(substr({{ telefone_limpo }}, 1, 2), {{ ddd_regex }})
            and char_length({{ telefone_limpo }}) > 9
        )
    {%- endset -%}

    {%- set flag_todos_digitos_iguais -%}
        (
            length({{ telefone_limpo }}) > 0
            and repeat(substr({{ telefone_limpo }}, 1, 1), length({{ telefone_limpo }})) = {{ telefone_limpo }}
        )
    {%- endset -%}

    {%- set telefone_formatado -%}
        case
            when length({{ telefone_limpo }}) = 11
                 and {{ ddd_valido }}
                 and substr({{ telefone_limpo }}, 3, 1) = '9'
                 and not {{ flag_todos_digitos_iguais }}
              then concat('55', {{ telefone_limpo }})

            when length({{ telefone_limpo }}) = 10
                 and {{ ddd_valido }}
                 and substr({{ telefone_limpo }}, 3, 1) between '6' and '9'
                 and not {{ flag_todos_digitos_iguais }}
              then concat('55', substr({{ telefone_limpo }}, 1, 2), '9', substr({{ telefone_limpo }}, 3))

            when length({{ telefone_limpo }}) = 13
                 and substr({{ telefone_limpo }}, 1, 2) = '55'
                 and regexp_contains(substr({{ telefone_limpo }}, 3, 2), {{ ddd_regex }})
                 and substr({{ telefone_limpo }}, 5, 1) = '9'
                 and not {{ flag_todos_digitos_iguais }}
              then {{ telefone_limpo }}

            else null
        end
    {%- endset -%}

    {%- set flag_telefone_formatado_nulo -%}
        ({{ telefone_formatado }} is null)
    {%- endset -%}

    {%- set flag_numero_compartilhado -%}
        case
            when {{ telefone_limpo }} is null then false
            else coalesce({{ total_cpfs_column }}, 0) >= 10
        end
    {%- endset -%}

    {%- set flag_texto_indefinido -%}
        upper({{ telefone_str }}) in (
            'NAO INFORMADO', 'NAO TEM', 'NAO POSSUI', 'NONE', 'SEM INFORMACAO', 'SEM TELEFONE',
            'NAO TEM CELULAR', 'NAO INFORMOU', 'SEM INF', 'SEM TELEFONE NO MOMENTO', 'SEM CONTATO',
            'S/TEL', 'S/N', 'SN', '-', 'X', 'XX', 'XXX', 'XXXX', 'XXXXX', 'XXXXXX', 'XXXXXXX',
            'XXXXXXXX', 'XXXXXXXXX', 'XXXXXXXXXX', 'XXXXXXXXXXXX', '()'
        )
    {%- endset -%}

    {%- set flag_poucos_digitos -%}
        length({{ telefone_limpo }}) < 8
    {%- endset -%}

    {%- set flag_repetidos_8_ou_mais -%}
        regexp_contains({{ telefone_limpo }}, r'0{8,}|1{8,}|2{8,}|3{8,}|4{8,}|5{8,}|6{8,}|7{8,}|8{8,}|9{8,}')
    {%- endset -%}

    {%- set flag_ddd_invalido -%}
        not {{ ddd_valido }}
    {%- endset -%}

    {%- set flag_celular_9d_digito_invalido -%}
        (
            length({{ telefone_limpo }}) = 11
            and substr({{ telefone_limpo }}, 3, 1) != '9'
        )
    {%- endset -%}

    {%- set flag_numero_institucional -%}
        (
            regexp_contains({{ telefone_limpo }}, r'^(0800|0300|0500|400[0-9])')
            or {{ telefone_limpo }} in ('2134601746', '6133152425')
        )
    {%- endset -%}

    struct(
        case
            when {{ telefone_column }} is null then null
            when
                {{ flag_telefone_formatado_nulo }}
                or {{ flag_numero_compartilhado }}
                or {{ flag_texto_indefinido }}
                or {{ flag_poucos_digitos }}
                or {{ flag_todos_digitos_iguais }}
                or {{ flag_repetidos_8_ou_mais }}
                or {{ flag_ddd_invalido }}
                or {{ flag_celular_9d_digito_invalido }}
                or {{ flag_numero_institucional }}
                or coalesce({{ flag_telefone_clinica_column }}, false)
            then null
            else {{ telefone_formatado }}
        end as telefone_valido_whatsapp,

        case
            when {{ telefone_column }} is null then null
            when coalesce({{ flag_telefone_clinica_column }}, false) then 'telefone_clinica'
            when {{ flag_numero_compartilhado }} then 'numero_compartilhado'
            when {{ flag_texto_indefinido }} then 'texto_indefinido'
            when {{ flag_poucos_digitos }} then 'poucos_digitos'
            when {{ flag_todos_digitos_iguais }} then 'todos_digitos_iguais'
            when {{ flag_repetidos_8_ou_mais }} then 'repetidos_8_ou_mais'
            when {{ flag_numero_institucional }} then 'numero_institucional'
            when {{ flag_celular_9d_digito_invalido }} then 'celular_9d_digito_invalido'
            when {{ flag_ddd_invalido }} then 'ddd_invalido'
            when {{ flag_telefone_formatado_nulo }} then 'telefone_formatado_nulo'
            else null
        end as motivo_invalidacao_telefone
    )

{% endmacro %}