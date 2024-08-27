{{
    config(
        schema="saude_historico_clinico",
        alias="alergia",
        materialized="table",
        cluster_by="paciente_cpf",
    )
}}

with
    vitai as (
        select id_paciente, cns, cpf, alergia
        from
            {{ ref("int_historico_clinico__alergia__vitai") }},
            unnest(alergias) as alergia
    ),
    vitacare as (
        select
            safe_cast(null as string) as id_paciente,
            safe_cast(null as string) as cns,
            cpf,
            alergia
        from
            {{ ref("int_historico_clinico__alergia__vitacare") }},
            unnest(alergias) as alergia
    ),
    total as (
        select *
        from vitai
        union all
        select *
        from vitacare
    ),

    final as (
        select
            cpf as paciente_cpf,
            array_agg(distinct alergia) as alergias,
            -- array_agg(distinct cns ignore nulls) as cns,
            struct(current_timestamp() as processed_at) as metadados
        from total
        where cpf is not null
        group by cpf
    )

select *
from final