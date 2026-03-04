{{
    config(
        alias="vacinacao_sipni",
        materialized="table",
        schema="intermediario_historico_clinico",
        partition_by={
            "field": "particao_registro_vacinacao",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}

with 
    estabelecimento as (
        select
            id_cnes,
            nome_limpo
        from {{ ref('dim_estabelecimento') }}
    ),

    cadastro_vitacare as (
        select 
            * 
        from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
    ),

    vacina as (
        select 
            * 
        from {{ ref('raw_sipni__vacinacao') }}
    ),

    joined as (
        select 
            v.*,
            c.codigo_equipe,
            c.ine_equipe,
            c.microarea,
            c.equipe,
            c.situacao_usuario,
            row_number() over (
                partition by 
                    safe_cast(v.paciente_cpf as string),
                    safe_cast(v.vacina_aplicacao_data as date),
                    lower(safe_cast(v.vacina_nome as string))
                order by
                    case 
                        when c.id_cnes = v.id_cnes then 1
                        else 2
                    end,
                    greatest(
                        c.data_cadastro,
                        c.data_atualizacao_cadastro,
                        c.updated_at
                    ) desc
            ) as rn
        from vacina v
        left join cadastro_vitacare c
            on c.cpf = safe_cast(v.paciente_cpf as string)
    ),

    casted_normalized as (
        select 
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "v.id_cnes",
                        "paciente_cpf",
                        "vacina_aplicacao_data",
                        "vacina_nome",
                        "vacina_dose",
                        "vacina_lote"
                    ]
                )
            }} as id_vacinacao,
            safe_cast(v.id_cnes as string) as id_cnes,
            safe_cast(v.codigo_equipe as string) as id_equipe,
            safe_cast(v.ine_equipe as string) as id_ine_equipe,
            safe_cast(v.microarea as string) as id_microarea,
            safe_cast(null as string) as paciente_id_prontuario,
            safe_cast(v.paciente_cns as string) as paciente_cns,
            safe_cast(v.paciente_cpf as string) as paciente_cpf,
            {{ proper_estabelecimento("safe_cast(e.nome_limpo as string)") }} as estabelecimento_nome,
            safe_cast(v.equipe as string) as equipe_nome,   
            {{ proper_br("safe_cast(v.profissional_nome as string)") }} as profissional_nome,
            safe_cast(null as string) as profissional_cbo,
            safe_cast(null as string) as profissional_cns,
            safe_cast(null as string) as profissional_cpf,
            lower(safe_cast(v.vacina_nome as string)) as vacina_descricao,
            lower(
                safe_cast(
                    {{ remove_accents_upper("replace(replace(v.vacina_dose, 'º', ''), 'ª', '')") }}
                as string)
            ) as vacina_dose,
            safe_cast(v.vacina_lote as string) as vacina_lote,
            safe_cast(vacina_tipo_registro as string) as vacina_registro_tipo,
            safe_cast(vacina_estrategia as string) as vacina_estrategia,
            safe_cast(null as string) as vacina_diff,
            safe_cast(v.vacina_aplicacao_data as date) as vacina_aplicacao_data,
            safe_cast(v.vacina_aplicacao_data as date) as vacina_registro_data,
            {{ proper_br("safe_cast(v.paciente_nome as string)") }} as paciente_nome,
            safe_cast(v.paciente_sexo as string) as paciente_sexo,
            safe_cast(v.paciente_nascimento_data as date) as paciente_nascimento_data,
            {{ proper_br("safe_cast(v.paciente_nome_mae as string)") }} as paciente_nome_mae,
            safe_cast(null as date) as paciente_mae_nascimento_data,
            safe_cast(v.situacao_usuario as string) as paciente_situacao,
            safe_cast(null as date) as paciente_cadastro_data,
            safe_cast(null as boolean) as paciente_obito,
            safe_cast(loaded_at as datetime) as loaded_at,
            safe_cast(v.vacina_aplicacao_data as date) as particao_registro_vacinacao
        from joined v
        left join estabelecimento e
            on e.id_cnes = v.id_cnes 
        where v.rn = 1
    )
    
select 
    *
from casted_normalized
