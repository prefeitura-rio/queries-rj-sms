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
                (trim(lower(descricao)) in ('outros', 'medicamento', 'outras')),
                regexp_replace(observacao, r'[!?\-"#*]', ''),
                (regexp_replace(descricao, r'[!?\-"#*]', ''))
            ) as agg_alergia
        from {{ ref("raw_prontuario_vitai__alergia") }}

    ),
    paciente_mrg as (
        select cpf, c as cns
        from {{ ref("mart_historico_clinico__paciente")}}, unnest(cns) as c
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
    fix_double as (
        select 
            gid_paciente,
            regexp_replace(upper(descricao), r' {2,}[A|E] {2,}', ' A ') as alergias_fix
        from alergias_std
    ),
    alergias_delimitadores as (
        select
            gid_paciente,
            regexp_replace(upper(alergias_fix), r'\+|\n|,| E |\\|\/|;|  ', '|') as alergias_sep
        from fix_double
    ),
    alergias_separadas as (
        select gid_paciente, split(alergias_sep, '|') as partes
        from alergias_delimitadores
    ),
    alergias_cleaned as (
        select
            gid_paciente,
            regexp_replace(
                upper(parte),
                'A{0,1}L[E|É]RGI[A|C][O|A]{0,1} {1,2}[H]{0,1}[A|À]{0,1}O{0,1}:{0,1} |ALEGA|AFIRMA|^A ',
                ''
            ) as alergias_clean
        from alergias_separadas, unnest(partes) as parte
    ),
    alergias_trimmed as (
        select gid_paciente, initcap(trim(alergias_clean)) as descricao
        from alergias_cleaned
    ),
    alergias_agg as (
        select gid_paciente, array_agg(distinct descricao ignore nulls) as alergias
        from alergias_trimmed
        where descricao is not null and descricao != ""
        group by 1
    ),
    boletim as (
        select distinct
            gid_paciente,
            if(cns in ('', 'None'), null, cns) as cns,
            if(cpf in ('', 'None'), null, cpf) as cpf,
        from {{ ref("raw_prontuario_vitai__boletim") }} 
    )

select 
    alergias_agg.gid_paciente as id_paciente, 
    boletim.cns, 
    IF(boletim.cpf is null,paciente_mrg.cpf,boletim.cpf) as cpf, 
    alergias
from alergias_agg
left join boletim on boletim.gid_paciente = alergias_agg.gid_paciente
left join paciente_mrg on boletim.cns = paciente_mrg.cns
