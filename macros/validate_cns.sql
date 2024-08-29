{% macro validate_cns(cns_column) %}
        CASE
            WHEN LENGTH(TRIM({{ cns_column }})) != 15 THEN FALSE

            -- Validation for CNS starting with 1 or 2
            WHEN SAFE_CAST(SUBSTR({{ cns_column }}, 1, 1) AS INT64) IN (1, 2) THEN (
                SELECT
                cns = CASE
                    WHEN 11 - resto = 10 THEN pis || '001' || CAST(dv AS STRING)
                    ELSE pis || '000' || CAST(dv AS STRING)
                END
                FROM (
                SELECT 
                    *,
                    CASE 
                        WHEN 11 - resto = 11 THEN 0
                        WHEN 11 - resto = 10 THEN 
                            11 - MOD((d1 * 15 + d2 * 14 + d3 * 13 + d4 * 12 + d5 * 11 + 
                                    d6 * 10 + d7 * 9 + d8 * 8 + d9 * 7 + d10 * 6 + d11 * 5 + 2), 11)
                        ELSE 11 - resto
                    END AS dv
                FROM (
                    SELECT
                    *,
                    MOD((d1 * 15 + d2 * 14 + d3 * 13 + d4 * 12 + d5 * 11 + 
                            d6 * 10 + d7 * 9 + d8 * 8 + d9 * 7 + d10 * 6 + d11 * 5), 11) AS resto
                    FROM (
                    SELECT
                        {{ cns_column }} AS cns,
                        SUBSTR({{ cns_column }}, 1, 11) AS pis,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 1, 1) AS INT64) AS d1,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 2, 1) AS INT64) AS d2,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 3, 1) AS INT64) AS d3,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 4, 1) AS INT64) AS d4,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 5, 1) AS INT64) AS d5,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 6, 1) AS INT64) AS d6,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 7, 1) AS INT64) AS d7,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 8, 1) AS INT64) AS d8,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 9, 1) AS INT64) AS d9,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 10, 1) AS INT64) AS d10,
                        SAFE_CAST(SUBSTR({{ cns_column }}, 11, 1) AS INT64) AS d11
                    )
                )
                )
            )

            -- Validation for CNS starting with 7, 8, or 9
            WHEN SAFE_CAST(SUBSTR({{ cns_column }}, 1, 1) AS INT64) IN (7, 8, 9) THEN (
                SELECT
                MOD((d1 * 15 + d2 * 14 + d3 * 13 + d4 * 12 + d5 * 11 + 
                        d6 * 10 + d7 * 9 + d8 * 8 + d9 * 7 + d10 * 6 +
                        d11 * 5 + d12 * 4 + d13 * 3 + d14 * 2 + d15 * 1), 11) = 0
                FROM (
                SELECT
                    {{ cns_column }} AS cns,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 1, 1) AS INT64) AS d1,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 2, 1) AS INT64) AS d2,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 3, 1) AS INT64) AS d3,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 4, 1) AS INT64) AS d4,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 5, 1) AS INT64) AS d5,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 6, 1) AS INT64) AS d6,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 7, 1) AS INT64) AS d7,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 8, 1) AS INT64) AS d8,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 9, 1) AS INT64) AS d9,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 10, 1) AS INT64) AS d10,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 11, 1) AS INT64) AS d11,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 12, 1) AS INT64) AS d12,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 13, 1) AS INT64) AS d13,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 14, 1) AS INT64) AS d14,
                    SAFE_CAST(SUBSTR({{ cns_column }}, 15, 1) AS INT64) AS d15
                )
            )
            ELSE FALSE
        END{% endmacro %}