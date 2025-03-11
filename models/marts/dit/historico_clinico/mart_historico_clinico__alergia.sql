{{
    config(
        schema="saude_historico_clinico",
        alias="alergia",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    vitai as (
        select 
            id_paciente, 
            cns, 
            cpf, 
            alergia.descricao_raw as alergia, 
            safe_cast(cpf as int64) as cpf_particao
        from
            {{ ref("int_historico_clinico__alergia__vitai") }},
            unnest(alergias) as alergia
    ),
    vitacare as (
        select
            safe_cast(null as string) as id_paciente,
            safe_cast(null as string) as cns,
            cpf,
            alergia,
            safe_cast(cpf as int64) as cpf_particao
        from
            {{ ref("int_historico_clinico__alergia__vitacare") }},
            unnest(alergias) as alergia
    ),
    total as (
        select *
        from (select * from vitai where alergia is not null)
        union all
        select *
        from vitacare
    ),

    final as (
        select
            cpf as paciente_cpf,
            cpf_particao,
            array_agg(distinct alergia) as alergias,
            -- array_agg(distinct cns ignore nulls) as cns,
            struct(current_timestamp() as processed_at) as metadados,
        from total
        where cpf is not null
        group by cpf, cpf_particao
    )

select paciente_cpf, alergias, metadados, cpf_particao
from final
