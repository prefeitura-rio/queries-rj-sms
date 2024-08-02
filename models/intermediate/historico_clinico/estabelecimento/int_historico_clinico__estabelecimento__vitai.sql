with
    estabelecimento_vitai as (
        select 
            gid, 
            cnes, 
            initcap(nome_estabelecimento) as nome_estabelecimento_cap
        from {{ ref("raw_prontuario_vitai__m_estabelecimento") }}
    )
select 
    gid, 
    cnes, 
    {{ proper_estabelecimento(nome_estabelecimento_cap) }} as nome_estabelecimento
from estabelecimento_vitai