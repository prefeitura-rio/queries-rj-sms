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
                    safe_cast(v.nu_cpf_paciente as string),
                    safe_cast(v.dt_vacina as date),
                    lower(safe_cast(v.ds_vacina as string))
                order by
                    case 
                        when c.id_cnes = safe_cast(v.co_cnes_estabelecimento as string) then 1
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
            on c.cpf = safe_cast(v.nu_cpf_paciente as string)
    ),

    casted_normalized as (
        select 
            safe_cast(null as string) as id_vacinacao,
            safe_cast(v.co_cnes_estabelecimento as string) as id_cnes,
            safe_cast(v.codigo_equipe as string) as id_equipe,
            safe_cast(v.ine_equipe as string) as id_ine_equipe,
            safe_cast(v.microarea as string) as id_microarea,
            safe_cast(null as string) as paciente_id_prontuario,
            safe_cast(v.nu_cns_paciente as string) as paciente_cns,
            safe_cast(v.nu_cpf_paciente as string) as paciente_cpf,
            safe_cast(e.nome_limpo as string) as estabelecimento_nome,
            safe_cast(v.equipe as string) as equipe_nome,   
            safe_cast(null as string) as profissional_nome,
            safe_cast(null as string) as profissional_cbo,
            safe_cast(null as string) as profissional_cns,
            safe_cast(null as string) as profissional_cpf,
            lower(safe_cast(v.ds_vacina as string)) as vacina_descricao,
            lower(
                safe_cast(
                    {{ remove_accents_upper("replace(replace(v.ds_dose_vacina, 'º', ''), 'ª', '')") }}
                as string)
            ) as vacina_dose,
            safe_cast(v.co_lote_vacina as string) as vacina_lote,
            safe_cast(null as string) as vacina_registro_tipo,
            safe_cast(null as string) as vacina_estrategia,
            safe_cast(null as string) as vacina_diff,
            safe_cast(v.dt_vacina as date) as vacina_aplicacao_data,
            safe_cast(null as date) as vacina_registro_data,
            safe_cast(v.no_paciente as string) as paciente_nome,
            case when safe_cast(v.tp_sexo_paciente as string) = 'M' then 'masculino'
                 when safe_cast(v.tp_sexo_paciente as string) = 'F' then 'feminino'
                 else null 
            end as paciente_sexo,
            safe_cast(v.dt_nascimento_paciente as date) as paciente_nascimento_data,
            safe_cast(v.no_mae_paciente as string) as paciente_nome_mae,
            safe_cast(null as date) as paciente_mae_nascimento_data,
            safe_cast(v.situacao_usuario as string) as paciente_situacao,
            safe_cast(null as date) as paciente_cadastro_data,
            safe_cast(null as boolean) as paciente_obito,
            safe_cast(null as datetime) as loaded_at,
            safe_cast(v.dt_vacina as date) as particao_registro_vacinacao
        from joined v
        left join estabelecimento e
            on e.id_cnes = safe_cast(v.co_cnes_estabelecimento as string)
        where v.rn = 1
    )
    
select 
    *
from casted_normalized
