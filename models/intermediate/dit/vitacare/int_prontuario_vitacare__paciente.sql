{{
    config(
        schema="intermediario_prontuario_vitacare",
        alias="paciente",
        materialized="table",
        tags=['daily']
    )
}}

select
    paciente.* replace (
        coalesce(paciente.cpf, enriquecimento.cpf_enriquecido) as cpf,
        {{
            validate_cpf(
                "coalesce(paciente.cpf, enriquecimento.cpf_enriquecido)"
            )
        }} as cpf_valido_indicador
    )
from {{ ref("raw_prontuario_vitacare__paciente") }} as paciente
left join {{ ref("int_prontuario_vitacare__paciente_cpf_enriquecido") }} as enriquecimento
    on paciente.id_paciente_global = enriquecimento.id_paciente_global
where
    coalesce(paciente.cpf, enriquecimento.cpf_enriquecido) is not null
    and paciente.id_cnes is not null
    and not regexp_contains(upper(paciente.nome), r'\bTESTE\b')
