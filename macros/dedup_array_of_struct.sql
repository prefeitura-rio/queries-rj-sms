{% macro dedup_array_of_struct(val) %}
 ( 
  select array_agg(t)
  from (select distinct * from unnest({{val}}) v) t
 )
{% endmacro %}