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
            cns as cns_provisorio,
            updated_at
        from paciente
        union all
        select
            cns,
            cns_provisorio,
            updated_at
        from paciente_cns
    ),

    todos_telefones as (
        select
            cns,
            telefone,
            tp_telefone,
            updated_at
        from paciente
        union all
        select
            cns,
            telefone,
            tipo_telefone as tp_telefone,
            cast(updated_at as datetime)
        from paciente_telefones
    ),

    -- --------------------------------------------------------------------------
    -- Agrupamento de dados
    -- --------------------------------------------------------------------------
    telefone_lista as (
        select
            cns,
            array_agg(telefone ignore nulls order by updated_at desc) as telefone_lista -- Ordenação de telefones pela data de atualização
        from todos_telefones
        group by cns
    ),

    cns_lista as (
        select
            cns,
            array_agg(cns_provisorio ignore nulls order by updated_at desc) as cns_provisorio_lista -- Ordenação de cns pela data de atualização
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
select 
    tp_telefone as tipo_telefone,
    tp_email as tipo_email,
    * except(tp_telefone, tp_email)
from joining