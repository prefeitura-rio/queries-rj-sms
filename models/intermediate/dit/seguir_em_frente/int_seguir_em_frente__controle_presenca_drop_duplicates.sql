-- remove duplicates
with
    source as (
        select * from {{ ref("int_seguir_em_frente__controle_presenca_unpivot") }}
    ),

    ordered_result as (
        select
            *,
            row_number() over (
                partition by cpf, registro_data order by criado_em desc
            ) as row_number
        from source
    )

select *
from ordered_result
where row_number = 1
