{% macro get_last_partition_date(table_ref) %}
  {% set sql %}
    SELECT 
      CASE 
        WHEN length(MAX(partition_id)) = 8 THEN PARSE_DATE('%Y%m%d', MAX(partition_id))
        WHEN length(MAX(partition_id)) = 6 THEN PARSE_DATE('%Y%m', MAX(partition_id))
        WHEN length(MAX(partition_id)) = 4 THEN PARSE_DATE('%Y', MAX(partition_id))
        ELSE NULL
      END AS last_partition
    FROM `{{ table_ref.database }}.{{ table_ref.schema }}.INFORMATION_SCHEMA.PARTITIONS`
    WHERE table_name = '{{ table_ref.identifier }}'
      AND partition_id <> '__NULL__' AND partition_id <> '__UNPARTITIONED__'
  {% endset %}

  {% set result = run_query(sql) %}
  {% if execute %}
    {% set last_partition = result.columns[0].values()[0] %}
    {{ return(last_partition) }}
  {% else %}
    {{ return('1970-01-01') }}
  {% endif %}
{% endmacro %}