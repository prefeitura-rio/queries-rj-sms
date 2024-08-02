with
    profissionais_std as (
        select gid, cns, cpf, initcap(nome) as nome
        from {{ ref("raw_prontuario_vitai__profissional") }}
    )
select gid, cns, cpf, {{ proper_br('nome') }} as nome
from profissionais_std
