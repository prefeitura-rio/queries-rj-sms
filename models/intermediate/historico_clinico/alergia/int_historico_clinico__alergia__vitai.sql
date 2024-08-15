with
    alergias_std as (
        select
            gid,
            gid_boletim,
            gid_paciente,
            if(descricao = "", null, initcap(descricao)) as descricao
        from {{ ref("raw_prontuario_vitai__alergia") }}
    )

select *
from alergias_std
