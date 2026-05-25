{{
    config(
        schema="intermediario_cie",
        alias="vacinacao_historico",
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

    paciente as (
        select
            id_global,
            codigo_equipe,
            ine_equipe,
            microarea,
            npront,
            cns,
            cpf,
            equipe,
            nome,
            sexo,
            data_nascimento,
            nome_mae,
            situacao_usuario,
            data_cadastro,
            obito
        from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
        qualify row_number() over( partition by id_local, id_cnes order by greatest(data_cadastro, data_atualizacao_cadastro, updated_at) desc) = 1
    ),

    profissional as (
        select 
            id_global,
            profissional_nome,
            profissional_cbo,
            profissional_cns,
            profissional_cpf
        from {{ ref('raw_prontuario_vitacare_historico__profissional') }}
        qualify row_number() over( partition by id_global order by loaded_at desc
        ) = 1
    ),

    vacina as (
        select 
            * 
        from {{ ref('raw_prontuario_vitacare_historico__vacina') }}
    ),

    casted_normalized as (
        select 
            vac.id_vacinacao,
            vac.id_cnes,
            pac.codigo_equipe as id_equipe,
            pac.ine_equipe as id_ine_equipe,
            pac.microarea as id_microarea,
            pac.npront as paciente_id_prontuario,
            pac.cns as paciente_cns,
            pac.cpf as paciente_cpf,
            est.nome_limpo as estabelecimento_nome,
            lower(pac.equipe) as equipe_nome,
            {{ proper_br('pro.profissional_nome')}} as profissional_nome,
            pro.profissional_cbo,
            pro.profissional_cns,
            pro.profissional_cpf,
            lower(vac.nome_vacina) as vacina_descricao,
            lower({{ remove_accents_upper("replace(replace(vac.dose, 'º', ''), 'ª', '')") }}) as vacina_dose,
            vac.lote as vacina_lote,
            lower({{ remove_accents_upper('vac.tipo_registro') }}) as vacina_registro_tipo,
            lower(vac.estrategia_imunizacao) as vacina_estrategia,
            vac.diff as vacina_diff,
            vac.data_aplicacao  as vacina_aplicacao_data,
            safe_cast(vac.data_registro as date) as vacina_registro_data,
            {{ proper_br('pac.nome')}} as paciente_nome,
            lower(pac.sexo) as paciente_sexo,
            pac.data_nascimento as paciente_nascimento_data,
            pac.nome_mae as paciente_nome_mae,
            safe_cast(null as date) as paciente_mae_nascimento_data,
            pac.situacao_usuario as paciente_situacao,
            safe_cast(pac.data_cadastro as date) as paciente_cadastro_data,
            pac.obito as paciente_obito,
            safe_cast(vac.loaded_at as datetime) as loaded_at,
            safe_cast(vac.data_aplicacao as date) as particao_aplicacao_vacinacao
        from vacina vac
        left join paciente pac
            on vac.id_cadastro = pac.id_global
        left join profissional pro
            on vac.id_profissional = pro.id_global
        left join estabelecimento est
            on vac.id_cnes = est.id_cnes

    )

select 
    *
from casted_normalized
