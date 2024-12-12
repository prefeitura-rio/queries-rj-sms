{% macro calculate_jaccard(input1,input2) %}
(
        select 1-intersecao/uniao
            from (
                select 
                    {{input1}} as nome_1,
                    {{input2}} as nome_2, 
                    count(distinct agg) as uniao
                from unnest(ARRAY_CONCAT(split({{input1}},' '), split({{input2}},' '))) as agg
            ) as union_count
            left join (
                select 
                    {{input1}} as nome_1, 
                    {{input2}} as nome_2, 
                    count(distinct i1) as intersecao
                from unnest(split({{input1}},' ')) as i1
                inner join 
                    unnest(split({{input2}},' ')) as i2
                on i1 = i2
            ) as intersection_count
            on concat(union_count.nome_1,'.',union_count.nome_2) = concat(intersection_count.nome_1,'.',intersection_count.nome_2)
)
{% endmacro %}