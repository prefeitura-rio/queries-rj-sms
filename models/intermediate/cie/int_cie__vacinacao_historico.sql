{{
    config(
        alias="vacinacao_historico",
        schema="intermediario_cie",
        materialized="table",
        partition_by={
            "field": "particao_data_vacinacao",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}

with
    estabelecimento as (
        select 
            id_cnes,
            nome_limpo as estabelecimento_nome
        from {{ ref('dim_estabelecimento') }}
    ),
    vacinacoes_historico as (
        select 
            * 
        from {{ ref('raw_prontuario_vitacare_historico__vacina') }}
    ),
    atendimentos_historico as (
        select 
            id_cnes,
            id_prontuario_global,
            id_local as ut_id,
            profissional_nome,
            profissional_equipe_nome,
            profissional_equipe_cod_ine,
            profissional_cbo,
            profissional_cns,
            profissional_cpf,
            patient_cpf,
        from {{ ref('raw_prontuario_vitacare_historico__acto') }}
    ),
    pacientes_historico as (
        select 
            id_cnes,
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
            obito,
            id_local as ut_id
        from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
        qualify row_number() over(
            partition by id_local, id_cnes
            order by greatest(data_cadastro, data_atualizacao_cadastro, updated_at) desc
        ) = 1
    ),

    agg_renamed as (
        select 
           id_vacinacao,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_vacinacao"
                    ]
                )
            }} as id_surrogate,
            v.id_cnes,
            p.codigo_equipe as id_equipe,
            a.profissional_equipe_cod_ine as id_equipe_ine,
            p.microarea as id_microarea,
            p.npront as paciente_id_prontuario,
            p.cns as paciente_cns,
            a.patient_cpf as paciente_cpf,
            e.estabelecimento_nome,
            a.profissional_equipe_nome as equipe_nome,
            a.profissional_nome,
            a.profissional_cbo,
            a.profissional_cns,
            a.profissional_cpf,
            lower(v.nome_vacina) as vacina_descricao,
            lower(replace(replace(v.dose, 'ª', ''),'º', '')) as vacina_dose,
            v.lote as vacina_lote,
            lower(v.tipo_registro) as vacina_registro_tipo,
            v.estrategia_imunizacao as vacina_estrategia,
            v.diff as vacina_diff,
            v.data_aplicacao as vacina_aplicacao_data,
            v.data_registro as vacina_registro_data,
            p.nome as paciente_nome,
            p.sexo as paciente_sexo,
            p.data_nascimento as paciente_nascimento_data,
            p.nome_mae as paciente_nome_mae,
            safe_cast(null as date) as paciente_mae_nascimento_data,
            p.situacao_usuario as paciente_situacao,
            p.data_cadastro as paciente_cadastro_data,
            safe_cast(p.obito as string) as paciente_obito,
            v.id_cnes as requisicao_id_cnes,
            safe_cast(null as string) as requisicao_area_programatica,
            safe_cast(null as string) as requisicao_endpoint,
            struct(
                safe_cast(null as datetime) as updated_at,
                safe_cast(null as datetime) as extracted_at,
                safe_cast(null as timestamp) as loaded_at
            ) as metadados,
            v.data_aplicacao as particao_data_vacinacao
        from vacinacoes_historico v
        left join atendimentos_historico a 
            on v.id_prontuario_global = a.id_prontuario_global
        left join pacientes_historico p
            on a.ut_id = p.ut_id
            and a.id_cnes = p.id_cnes
        left join estabelecimento e
            on v.id_cnes = e.id_cnes
    )

select * 
from agg_renamed