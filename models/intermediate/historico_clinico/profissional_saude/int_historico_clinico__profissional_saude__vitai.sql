with
    profissionais_std as (
        select gid, cns, cpf, initcap(nome) as nome, cbo_descricao
        from {{ ref("raw_prontuario_vitai__profissional") }}
    )
select gid, cns, cpf, {{ proper_br('nome') }} as nome, cbo_descricao
from profissionais_std
