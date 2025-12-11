{% macro cdi_clean_text(field) -%}
-- Remove quebras de linha, tabs e espaços extras
REGEXP_REPLACE(
  REGEXP_REPLACE(
    REGEXP_REPLACE(TRIM(CAST({{ field }} AS STRING)), r'[\n\r\t\xA0]+', ' '),
    r'\s+', ' '
  ),
  r' +', ' '
)
{%- endmacro %}


-- Retorna string no formato "dd/mm/yyyy" (ou NULL)
{% macro cdi_date_str(field) -%}
(
  CASE
    WHEN {{ normalize_null(field) }} IS NULL THEN NULL
    ELSE
      CASE
        -- Corrige "0904/2025" → "09/04/2025"
        WHEN REGEXP_CONTAINS({{ cdi_clean_text(field) }}, r'^\d{4}/\d{4}$') THEN
          CONCAT(SUBSTR({{ cdi_clean_text(field) }}, 1, 2), '/', SUBSTR({{ cdi_clean_text(field) }}, 3))

        -- Corrige "904/2025" → "09/04/2025"
        WHEN REGEXP_CONTAINS({{ cdi_clean_text(field) }}, r'^\d{3}/\d{4}$') THEN
          CONCAT('0', SUBSTR({{ cdi_clean_text(field) }}, 1, 1), '/', SUBSTR({{ cdi_clean_text(field) }}, 2))

        -- Corrige "31/03//2025" → "31/03/2025"
        WHEN REGEXP_CONTAINS({{ cdi_clean_text(field) }}, r'//') THEN
          REGEXP_REPLACE({{ cdi_clean_text(field) }}, r'//', '/')

        ELSE
          {{ cdi_clean_text(field) }}
      END
  END
)
{%- endmacro %}


-- Converte para DATE e retorna a última data válida
{% macro cdi_parse_date(field, processo_field=None, oficio_field=None) -%}
(
  SAFE.PARSE_DATE(
    '%d/%m/%Y',
    CASE
      WHEN {{ field }} IS NULL THEN NULL

      -- Corrige "14/04/0205" → "14/04/2025"
      WHEN REGEXP_CONTAINS({{ field }}, r'/0\d{3}$') THEN
        CASE
          WHEN ({{ processo_field }} LIKE '%2025%' OR {{ oficio_field }} LIKE '%2025%')
            THEN REGEXP_REPLACE({{ field }}, r'/0\d{3}$', '/2025')
          ELSE {{ field }}
        END

      -- Extrai e retorna a última data válida do campo
      ELSE
        (
          SELECT
            ARRAY_AGG(d ORDER BY SAFE.PARSE_DATE('%d/%m/%Y', d) DESC LIMIT 1)[OFFSET(0)]
          FROM UNNEST(
            REGEXP_EXTRACT_ALL(
              REGEXP_REPLACE(TRIM(CAST({{ field }} AS STRING)), r'[\xA0\s\t]+', ' '),
              r'\d{1,2}/\d{1,2}/\d{4}'
            )
          ) AS d
          WHERE SAFE.PARSE_DATE('%d/%m/%Y', d) IS NOT NULL
        )
    END
  )
)
{%- endmacro %}