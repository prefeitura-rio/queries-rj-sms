-- Check if there are negative values in the stock position table
with
    posicao as (
        select
            *,
            safe_cast(saldo as float64) as produto_quantidade,
            safe_cast(valormedio as float64) as produto_valor_unitario
        from {{ source("raw_prontuario_vitai_staging", "estoque_posicao") }}
    )

select *
from posicao
where produto_quantidade < 0 or produto_valor_unitario < 0
