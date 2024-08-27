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
            cns as paciente_cns,
            cpf as paciente_cpf,
            array_agg(alergia) as alergias,
            struct(current_timestamp() as created_at) as metadados
        from total
        group by id_paciente, cns, cpf
        having {{ validate_cpf("cpf") }} or cns is not null
    )

-- select paciente_cpf, count(1) as records
-- from final
-- group by 1
-- having records > 1
select *
from final
where paciente_cpf is null
