{{
    config(
        alias="paciente_cadastro",
        tags="smsrio"
    )
}}

with
    paciente as (
        select * from {{ ref('raw_plataforma_smsrio__paciente') }}
    ),
    paciente_cns as (
        select * from {{ ref('raw_plataforma_smsrio__paciente_cns') }}
    ),
    paciente_telefones as (
        select * from {{ ref('raw_plataforma_smsrio__paciente_telefones') }}
    ),
    -- --------------------------------------------------------------------------
    -- Enriquecimento de dados
    -- Os principais dados de cada paciente são os que estão no cadastro.
    -- --------------------------------------------------------------------------
    todos_cns as (
        select
            cns,
            cns as cns_provisorio
        from paciente
        union all
        select
            cns,
            cns_provisorio
        from paciente_cns
    ),
    todos_telefones as (
        select
            cns,
            telefone,
            tp_telefone
        from paciente
        union all
        select
            cns,
            telefone,
            tp_telefone
        from paciente_telefones
    ),
    -- --------------------------------------------------------------------------
    -- Agrupamento de dados
    -- --------------------------------------------------------------------------
    telefone_lista as (
        select
            cns,
            array_agg(telefone ignore nulls) as telefone_lista
        from todos_telefones
        group by cns
    ),
    cns_lista as (
        select
            cns,
            array_agg(cns_provisorio ignore nulls) as cns_provisorio_lista
        from todos_cns
        group by cns
    ),
    -- --------------------------------------------------------------------------
    -- Enriquecimento de dados
    -- --------------------------------------------------------------------------
    joining as (
        select
            paciente.*,
            array_to_string(coalesce(telefone_lista.telefone_lista, []), ',') as telefone_lista,
            array_to_string(coalesce(cns_lista.cns_provisorio_lista, []), ',') as cns_lista
        from paciente
            left join telefone_lista using (cns)
            left join cns_lista using (cns)
    )
select * 
from joining