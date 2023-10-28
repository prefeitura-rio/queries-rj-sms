-- Check if 
with
    posicao as (
        select
            *,
            safe_cast(saldo as float64) as material_quantidade,
            safe_cast(valormedio as float64) as material_valor_unitario
        from {{ source("raw_prontuario_vitai", "estoque_posicao") }}
    )

select *
from posicao
where material_quantidade < 0 or material_valor_unitario < 0
