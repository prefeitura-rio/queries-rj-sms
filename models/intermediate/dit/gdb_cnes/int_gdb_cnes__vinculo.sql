{{
    config(
        alias="int_gdb_cnes__vinculo",
        materialized="table",
        tags=["gdb_cnes"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with 
    vinculo as (
        select * from {{ ref("raw_gdb_cnes__vinculo") }}
        -- Verificar qual a melhor forma de filtrar os dados mais recentes, considerando que o mesmo profissional pode ter múltiplos vínculos
        qualify row_number() over (partition by id_profissional_cnes, id_unidade, id_vinculo, id_cbo order by data_carga desc) = 1
    )

select *
from vinculo