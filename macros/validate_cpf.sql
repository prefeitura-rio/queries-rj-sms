{% macro validate_cpf(cpf_column) %}
        -- cpf validation based on https://homepages.dcc.ufmg.br/~rodolfo/aedsi-2-10/regrasDigitosVerificadoresCPF.html
        CASE 
            WHEN LENGTH({{ cpf_column }}) != 11 THEN FALSE
            WHEN {{ cpf_column }} IN ('00000000000', '11111111111', '22222222222', '33333333333', 
                                    '44444444444', '55555555555', '66666666666', '77777777777', 
                                    '88888888888', '99999999999') THEN FALSE
            ELSE (
                SELECT 
                    CASE 
                        WHEN calculated_first_digit = d0 AND calculated_second_digit = d_unit THEN TRUE
                        ELSE FALSE
                    END
                FROM (
                    SELECT 
                        *,
                        CASE 
                            WHEN second_sum_mod < 2 THEN 0
                            ELSE 11 - second_sum_mod
                        END AS calculated_second_digit
                    FROM (
                        SELECT 
                            *,
                            MOD(d9*11 + d8*10 + d7*9 + d6*8 + d5*7 + d4*6 + d3*5 + d2*4 + d1*3 + calculated_first_digit*2, 11) AS second_sum_mod
                        FROM (
                            SELECT 
                                *,
                                CASE 
                                    WHEN first_sum_mod < 2 THEN 0
                                    ELSE 11 - first_sum_mod
                                END AS calculated_first_digit
                            FROM (
                                SELECT 
                                    *,
                                    MOD(d9*10 + d8*9 + d7*8 + d6*7 + d5*6 + d4*5 + d3*4 + d2*3 + d1*2, 11) AS first_sum_mod
                                FROM (
                                    SELECT 
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 1, 1) AS INT64) AS d9,
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 2, 1) AS INT64) AS d8,
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 3, 1) AS INT64) AS d7,
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 4, 1) AS INT64) AS d6,
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 5, 1) AS INT64) AS d5,
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 6, 1) AS INT64) AS d4,
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 7, 1) AS INT64) AS d3,
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 8, 1) AS INT64) AS d2,
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 9, 1) AS INT64) AS d1,
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 10, 1) AS INT64) AS d0,
                                        SAFE_CAST(SUBSTR({{ cpf_column }}, 11, 1) AS INT64) AS d_unit
                                )
                            )
                        )
                    )
                )
            )
        END{% endmacro %}