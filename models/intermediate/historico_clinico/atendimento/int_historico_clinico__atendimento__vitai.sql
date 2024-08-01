with
    profissionais_std as (
        select
            gid,
            gid_boletim,
            gid_estabelecimento,
            gid_paciente,
            gid_profissional,
            atendimento_tipo,
            especialidade,
            inicio_datahora,
            fim_datahora,
            if(cid_codigo = 'None', null, cid_codigo) as cid_codigo,
            if(cid_nome = 'None', null, cid_nome) as cid_nome
        from {{ ref("raw_prontuario_vitai__atendimento") }}
    )
select *
from profissionais_std
