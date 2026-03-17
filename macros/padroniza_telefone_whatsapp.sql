{% macro padroniza_telefone_whatsapp(telefone_column, total_cpfs_column='0', flag_telefone_clinica_column='false') %}

    /* Valor original convertido para string e com trim nas extremidades */
    {%- set telefone_str = "trim(cast(" ~ telefone_column ~ " as string))" -%}

    /* Remove qualquer caractere não numérico para trabalhar apenas com dígitos */
    {%- set telefone_digits = "regexp_replace(" ~ telefone_str ~ ", r'[^0-9]', '')" -%}

    /* Remove zeros à esquerda */
    {%- set telefone_limpo -%}
        regexp_replace({{ telefone_digits }}, r'^0+', '')
    {%- endset -%}

    /* Lista de DDDs válidos no Brasil */
    {%- set ddd_regex -%}
        r'^(11|12|13|14|15|16|17|18|19|21|22|24|27|28|31|32|33|34|35|37|38|41|42|43|44|45|46|47|48|49|51|53|54|55|61|62|63|64|65|66|67|68|69|71|73|74|75|77|79|81|82|83|84|85|86|87|88|89|91|92|93|94|95|96|97|98|99)$'
    {%- endset -%}

    /* Considera válido quando os 2 primeiros dígitos formam um DDD válido e o número possui ao menos 10 dígitos */
    {%- set ddd_valido -%}
        (
            regexp_contains(substr({{ telefone_limpo }}, 1, 2), {{ ddd_regex }})
            and char_length({{ telefone_limpo }}) > 9
        )
    {%- endset -%}

    /* Identifica números compostos inteiramente pelo mesmo dígito (ex.: 99999999999, 00000000000) */
    {%- set flag_todos_digitos_iguais -%}
        (
            length({{ telefone_limpo }}) > 0
            and repeat(substr({{ telefone_limpo }}, 1, 1), length({{ telefone_limpo }})) = {{ telefone_limpo }}
        )
    {%- endset -%}

    /* Padroniza para formato nacional com código do país (55 + DDD + número) de acordo com a resolução da Anatel nº 749 */
    {%- set telefone_formatado -%}
        case
            /* Caso 1: número com 11 dígitos */
            /* Estrutura esperada: DDD + número móvel */
            when length({{ telefone_limpo }}) = 11
                /* Verifica se o DDD é válido */
                and {{ ddd_valido }}
                /* Verifica se o identificador local é 7, 8 ou 9 */
                and substr({{ telefone_limpo }}, 3, 1) in ('7', '8', '9')
                /* Exclui números com todos os dígitos iguais */
                and not {{ flag_todos_digitos_iguais }}
            /* Adiciona o código do país 55 na frente */
            then concat('55', {{ telefone_limpo }})

            /* Caso 2: número com 10 dígitos */
            /* Estrutura esperada: DDD + número fixo */
            when length({{ telefone_limpo }}) = 10
                /* Verifica se o DDD é válido */
                and {{ ddd_valido }}
                /* Verifica se o primeiro dígito do identificador local indica telefone fixo */
                and substr({{ telefone_limpo }}, 3, 1) between '2' and '6'
                /* Exclui números com todos os dígitos iguais */
                and not {{ flag_todos_digitos_iguais }}
            /* Adiciona o código do país 55 na frente */
            then concat('55', {{ telefone_limpo }})

            /* Caso 3: número já vem com 13 dígitos */
            /* Estrutura esperada: 55 + DDD + número móvel */
            when length({{ telefone_limpo }}) = 13
                /* Verifica se começa com o código do Brasil 55 */
                and substr({{ telefone_limpo }}, 1, 2) = '55'
                /* Verifica se o DDD após o 55 é válido */
                and regexp_contains(substr({{ telefone_limpo }}, 3, 2), {{ ddd_regex }})
                /* Verifica se é número móvel */
                and substr({{ telefone_limpo }}, 5, 1) in ('7', '8', '9')
                /* Rejeita sequências com todos os dígitos iguais */
                and not {{ flag_todos_digitos_iguais }}
            then {{ telefone_limpo }}

            /* Caso 4: número já vem com 12 dígitos */
            /* Estrutura esperada: 55 + DDD + número fixo */
            when length({{ telefone_limpo }}) = 12
                /* Verifica se começa com o código do Brasil */
                and substr({{ telefone_limpo }}, 1, 2) = '55'
                /* Verifica se o DDD após o 55 é válido */
                and regexp_contains(substr({{ telefone_limpo }}, 3, 2), {{ ddd_regex }})
                /* Verifica se o dígito seguinte indica telefone fixo */
                and substr({{ telefone_limpo }}, 5, 1) between '2' and '6'
                /* Exclui números com todos os dígitos iguais */
                and not {{ flag_todos_digitos_iguais }}
            then {{ telefone_limpo }}

            /* Qualquer outro caso é tratado como nulo */
            else null
        end
    {%- endset -%}

    /* Identifica números que não puderam ser convertidos para um formato aceito para disparo e viraram nulo */
    {%- set flag_telefone_formatado_nulo -%}
        ({{ telefone_formatado }} is null)
    {%- endset -%}

    /* Marca como compartilhado quando o mesmo telefone aparece vinculado a muitos CPFs */
    {%- set flag_numero_compartilhado -%}
        case
            when {{ telefone_limpo }} is null then false
            else coalesce({{ total_cpfs_column }}, 0) >= 10
        end
    {%- endset -%}

    /* Identifica textos recorrentes que indicam ausência de telefone informado */
    {%- set flag_texto_indefinido -%}
        upper({{ telefone_str }}) in (
            'NAO INFORMADO', 'NAO TEM', 'NAO POSSUI', 'NONE', 'SEM INFORMACAO', 'SEM TELEFONE',
            'NAO TEM CELULAR', 'NAO INFORMOU', 'SEM INF', 'SEM TELEFONE NO MOMENTO', 'SEM CONTATO',
            'S/TEL', 'S/N', 'SN', '-', 'X', 'XX', 'XXX', 'XXXX', 'XXXXX', 'XXXXXX', 'XXXXXXX',
            'XXXXXXXX', 'XXXXXXXXX', 'XXXXXXXXXX', 'XXXXXXXXXXXX', '()'
        )
    {%- endset -%}

    /* Números muito curtos não são considerados telefones úteis para contato */
    {%- set flag_poucos_digitos -%}
        length({{ telefone_limpo }}) < 8
    {%- endset -%}

    /* Número com muitos dígitos repetidos */
    {%- set flag_repetidos_8_ou_mais -%}
        regexp_contains({{ telefone_limpo }}, r'0{8,}|1{8,}|2{8,}|3{8,}|4{8,}|5{8,}|6{8,}|7{8,}|8{8,}|9{8,}')
    {%- endset -%}

    /* DDD fora da lista válida */
    {%- set flag_ddd_invalido -%}
        not {{ ddd_valido }}
    {%- endset -%}

    /* Verifica se o número tem 11 dígitos no formato nacional, mas não tem o dígito 9 como identificador local */
    {%- set flag_celular_9d_digito_invalido -%}
        (
            length({{ telefone_limpo }}) = 11
            /* Verifica se o primeiro dígito após o DDD não é 9 */
            and substr({{ telefone_limpo }}, 3, 1) != '9'
        )
    {%- endset -%}

    /* Detecta números institucionais conhecidos ou prefixos de serviços de atendimento ao público */
    {%- set flag_numero_institucional -%}
        (
            regexp_contains({{ telefone_limpo }}, r'^(0800|0300|0500|400[0-9])')
            or {{ telefone_limpo }} in ('2134601746', '6133152425')
            /* 2134601746: Central 1746 de Atendimento ao Cidadão */
            /* 6133152425: Ministério da Saúde */
        )
    {%- endset -%}

    struct(
        case
            when {{ telefone_column }} is null then null
            when {{ flag_numero_compartilhado }} then null
            when {{ flag_texto_indefinido }} then null
            when {{ flag_poucos_digitos }} then null
            when {{ flag_todos_digitos_iguais }} then null
            when {{ flag_repetidos_8_ou_mais }} then null
            when {{ flag_numero_institucional }} then null
            when coalesce({{ flag_telefone_clinica_column }}, false) then null
            when {{ flag_celular_9d_digito_invalido }} then null
            when {{ flag_ddd_invalido }} then null
            when {{ flag_telefone_formatado_nulo }} then null
            else {{ telefone_formatado }}
        end as telefone_valido_whatsapp,

        case
            when {{ telefone_column }} is null then null
            when {{ flag_numero_compartilhado }} then 'numero_compartilhado'
            when {{ flag_texto_indefinido }} then 'texto_indefinido'
            when {{ flag_poucos_digitos }} then 'poucos_digitos'
            when {{ flag_todos_digitos_iguais }} then 'todos_digitos_iguais'
            when {{ flag_repetidos_8_ou_mais }} then 'repetidos_8_ou_mais'
            when {{ flag_numero_institucional }} then 'numero_institucional'
            when coalesce({{ flag_telefone_clinica_column }}, false) then 'telefone_clinica'
            when {{ flag_celular_9d_digito_invalido }} then 'celular_9d_digito_invalido'
            when {{ flag_ddd_invalido }} then 'ddd_invalido'
            when {{ flag_telefone_formatado_nulo }} then 'telefone_formatado_nulo'
            else null
        end as motivo_invalidacao_telefone
    )

{% endmacro %}