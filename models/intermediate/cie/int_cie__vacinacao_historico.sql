{{
    config(
        alias="vacinacao_historico",
        schema="intermediario_cie",
        materialized="table",
        partition_by={
            "field": "particao_aplicacao_vacinacao",
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

    vacina as (
        select 
            * 
        from {{ ref('raw_prontuario_vitacare_historico__vacina') }}
    ),

    profissional as (
        select 
            * 
        from {{ ref('raw_prontuario_vitacare_historico__profissional') }}
        qualify row_number() over( partition by id_global order by loaded_at desc
        ) = 1
    ),

    paciente as (
        select 
            * 
        from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
        qualify row_number() over( partition by id_local, id_cnes order by greatest(data_cadastro, data_atualizacao_cadastro, updated_at) desc
        ) = 1
    ),

    casted_normalized as (
        select 
            va.id_vacinacao,
            va.id_cnes,
            pa.codigo_equipe as id_equipe,
            pa.ine_equipe as id_ine_equipe,
            pa.microarea as id_microarea,
            pa.npront as paciente_id_prontuario,
            pa.cns as paciente_cns,
            pa.cpf as paciente_cpf,
            e.nome_limpo as estabelecimento_nome,
            lower(pa.equipe) as equipe_nome,
            {{ proper_br('pr.profissional_nome')}} as profissional_nome,
            pr.profissional_cbo,
            pr.profissional_cns,
            pr.profissional_cpf,
            lower(va.vacina_nome) as vacina_descricao,
            lower({{ remove_accents_upper("replace(replace(va.vacina_dose, 'º', ''), 'ª', '')") }}) as vacina_dose,
            va.vacina_lote as vacina_lote,
            lower({{ remove_accents_upper('va.vacina_tipo_registro') }}) as vacina_registro_tipo,
            lower(va.vacina_estrategia_imunizacao) as vacina_estrategia,
            va.vacina_diferenca_dias as vacina_diff,
            va.vacina_aplicacao_data  as vacina_aplicacao_data,
            safe_cast(va.vacina_registro_data as date) as vacina_registro_data,
            {{ proper_br('pa.nome')}} as paciente_nome,
            lower(pa.sexo) as paciente_sexo,
            pa.data_nascimento as paciente_nascimento_data,
            pa.nome_mae as paciente_nome_mae,
            safe_cast(null as date) as paciente_mae_nascimento_data,
            pa.situacao_usuario as paciente_situacao,
            safe_cast(pa.data_cadastro as date) as paciente_cadastro_data,
            pa.obito as paciente_obito,
            safe_cast(va.loaded_at as datetime) as loaded_at,
            safe_cast(va.vacina_aplicacao_data as date) as particao_aplicacao_vacinacao
        from vacina va
        left join profissional pr
            on va.id_profissional = pr.id_global
        left join paciente pa
            on va.id_cadastro = pa.id_global
        left join estabelecimento e
            on va.id_cnes = e.id_cnes
    )

select 
    *
from casted_normalized
