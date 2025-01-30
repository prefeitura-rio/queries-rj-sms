{% test assert_relacionamento_tabelas(
    model,column_name, field, to
) %}

with parent as (
    select
        {{ field }} as id
    from {{ to }}
),

child as (
    select
        {{ column_name }} as id
    from {{ model }}
),
error_threshold as (
    select
        cast(count(distinct id) * 0.1 as int) as threshold
    from parent
),
test as (
    select count(*) as not_matched_rows
    from child 
    where child.id is not null
    and child.id not in (select id from parent)
)
select 
    case 
        when not_matched_rows = 0 then 0
        when not_matched_rows > 0 and not_matched_rows <= (select threshold from error_threshold) then 1
        when not_matched_rows > (select threshold from error_threshold) then 2
    end as response 
from test

{% endtest %}


