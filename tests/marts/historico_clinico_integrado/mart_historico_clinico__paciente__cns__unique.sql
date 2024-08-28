-- verifica se o CNS está sendo usado em mais de um paciente
with
    cns_count as (
        select cns, count(distinct cpf) as cpfs_distintos
        from {{ ref("mart_historico_clinico__paciente") }}, unnest(cns) as cns
        group by 1
    )
select *
from cns_count
where cpfs_distintos > 1
order by cpfs_distintos desc
