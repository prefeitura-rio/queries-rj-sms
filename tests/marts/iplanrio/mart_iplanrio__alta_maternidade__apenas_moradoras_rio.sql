-- Garante que a base de disparo contenha apenas moradoras com municipio do Rio confirmado.
select
    id_evento_obstetrico,
    municipio,
    uf,
    prontuario_origem
from {{ ref('mart_iplanrio__alta_maternidade') }}
where coalesce(
    upper(trim(municipio)) in ('RIO DE JANEIRO', '3304557'),
    false
) = false
