{{
    config(
        schema="intermediario_prontuario_vitacare",
        alias="paciente_cpf_enriquecido",
        materialized="table",
        cluster_by=["id_paciente_global"],
        tags=["weekly"]
    )
}}

with
    pacientes_sem_cpf as (
        select
            id_paciente_global,
            nullif({{ remove_duplicate_whitespace(remove_accents_upper("nome")) }}, "") as nome_normalizado,
            data_nascimento,
            nullif({{ remove_duplicate_whitespace(remove_accents_upper("mae_nome")) }}, "") as mae_nome_normalizado
        from {{ ref("raw_prontuario_vitacare__paciente") }}
        where cpf is null
    ),

    pacientes_elegiveis as (
        select
            *,
            split(nome_normalizado, " ")[safe_offset(0)] as primeiro_nome_normalizado
        from pacientes_sem_cpf
        where
            nome_normalizado is not null
            and data_nascimento is not null
            and mae_nome_normalizado is not null
    ),

    pacientes_com_hashes as (
        select
            *,
            mod(abs(farm_fingerprint(primeiro_nome_normalizado)), 1024) as hash_particao,
            farm_fingerprint(
                concat(
                    nome_normalizado,
                    "|",
                    format_date("%F", data_nascimento),
                    "|",
                    mae_nome_normalizado
                )
            ) as hash_match
        from pacientes_elegiveis
    ),

    candidatos as (
        select
            paciente.id_paciente_global,
            count(distinct indice.cpf) as quantidade_cpf_candidatos,
            if(
                count(distinct indice.cpf) = 1,
                any_value(indice.cpf),
                null
            ) as cpf_enriquecido
        from pacientes_com_hashes as paciente
        left join {{ ref("int_bcadastro__cpf_indice") }} as indice
            on paciente.hash_particao = indice.hash_particao
            and paciente.hash_match = indice.hash_match
            and paciente.nome_normalizado = indice.nome_normalizado
            and paciente.data_nascimento = indice.data_nascimento
            and paciente.mae_nome_normalizado = indice.mae_nome_normalizado
        group by paciente.id_paciente_global
    )

select
    id_paciente_global,
    cpf_enriquecido,
    quantidade_cpf_candidatos,
    case
        when quantidade_cpf_candidatos = 0 then "SEM_CANDIDATO"
        when quantidade_cpf_candidatos = 1 then "CPF_UNICO"
        else "MULTIPLOS_CPFS"
    end as resultado_enriquecimento,
    current_timestamp() as processed_at
from candidatos
