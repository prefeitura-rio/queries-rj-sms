{{
    config(
        alias="vacinacao_api",
        materialized="table",
        schema="intermediario_historico_clinico",
        partition_by={
            "field": "particao_registro_vacinacao",
            "data_type": "date",
            "granularity": "day"
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

    vacina as (
        select 
            * 
        from {{ ref('raw_prontuario_vitacare_api_centralizadora__vacinacao') }}
    ),

    casted_normalized as (
        select 
            v.id_vacinacao,
            v.id_cnes,
            v.id_equipe,
            v.id_equipe_ine as id_ine_equipe,
            v.id_microarea,
            v.paciente_id_prontuario,
            v.paciente_cns,
            v.paciente_cpf,
            e.nome_limpo as estabelecimento_nome,
            lower(v.equipe_nome) as equipe_nome,
            {{ proper_br('v.profissional_nome')}} as profissional_nome,
            v.profissional_cbo,
            v.profissional_cns,
            v.profissional_cpf,
            v.vacina_descricao,
            v.vacina_dose,
            v.vacina_lote,
            case 
                when v.vacina_registro_tipo = 'registro de aplicacao anterior' then 'registro de vacinacao anterior'
                when v.vacina_registro_tipo = 'nao aplicavel' then 'nao aplicada'
                else lower(v.vacina_registro_tipo) 
            end as vacina_registro_tipo,
            lower(v.vacina_estrategia) as vacina_estrategia,
            v.vacina_diff,
            v.vacina_aplicacao_data,
            safe_cast(v.vacina_registro_data as date) as vacina_registro_data,
            {{ proper_br('v.paciente_nome')}} as paciente_nome,
            case 
                when v.paciente_sexo = 'f' then 'feminino'
                when v.paciente_sexo = 'm' then 'masculino'
                else null
            end as paciente_sexo,
            v.paciente_nascimento_data,
            v.paciente_nome_mae,
            v.paciente_mae_nascimento_data,
            v.paciente_situacao,
            safe_cast(v.paciente_cadastro_data as date) as paciente_cadastro_data,
            (coalesce(v.paciente_obito, '') != '') AS paciente_obito,
            safe_cast(v.metadados.loaded_at as datetime) as loaded_at,
            safe_cast(v.vacina_registro_data as date) as particao_registro_vacinacao
        from vacina v
        left join estabelecimento e
            on v.id_cnes = e.id_cnes
    )

select 
    *
from casted_normalized






