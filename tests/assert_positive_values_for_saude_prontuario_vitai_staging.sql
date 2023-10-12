-- Check if there are negative values in the stock position table
with
    posicao as (
        select
            *,
            safe_cast(saldo as float64) as material_quantidade,
            safe_cast(valormedio as float64) as material_valor_unitario
        from {{ source("brutos_prontuario_vitai_staging", "estoque_posicao") }}
    )

select *
from posicao
where material_quantidade < 0 or material_valor_unitario < 0
