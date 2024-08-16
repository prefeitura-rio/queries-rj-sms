{{
    config(
        schema="intermediario_historico_clinico",
        alias="alergias_vitai",
        materialized="table",
    )
}}
with
    get_alergias as (
        select
            gid,
            gid_paciente,
            gid_boletim,
            tipo,
            descricao,
            reacao,
            observacao,
            if(
                (
                    trim(lower(descricao))
                    in ('comentario', 'outros', 'medicamento', 'outras')
                ),
                regexp_replace(
                    observacao, r"[-'`~!@#$%^&*()_|=?;:'.<>\{\}\[\]\\\/]", ' '
                ),
                regexp_replace(
                    descricao, r"[-'`~!@#$%^&*()_|=?;:'.<>\{\}\[\]\\\/]", ' '
                )
            ) as agg_alergia
        from {{ ref("raw_prontuario_vitai__alergia") }}

    ),
    alergias_std as (
        select
            gid,
            gid_paciente,
            gid_boletim,
            if(
                (trim(agg_alergia) = "")
                or (trim(agg_alergia) = "None")
                or (regexp_contains(trim(agg_alergia), r'%{1,}'))
                or (trim(agg_alergia) = '0')
                or (length(trim(agg_alergia)) < 3)
                or (regexp_contains(trim(agg_alergia), r'\.{1,}')),
                null,
                initcap(trim(agg_alergia))
            ) as descricao
        from get_alergias
        where agg_alergia is not null
    ),
    alergias_agg as (
        select gid_paciente, array_agg(distinct descricao ignore nulls) as alergias
        from alergias_std
        where descricao is not null
        group by 1
    ),
    boletim as (
        select distinct
            gid_paciente,
            if(cns in ('', 'None'), null, cns) as cns,
            if(cpf in ('', 'None'), null, cpf) as cpf
        from {{ ref("raw_prontuario_vitai__boletim") }}
    )

select alergias_agg.gid_paciente as id_paciente, cns, cpf, alergias
from alergias_agg
left join boletim on boletim.gid_paciente = alergias_agg.gid_paciente
