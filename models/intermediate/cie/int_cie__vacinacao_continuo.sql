{{
    config(
        alias="vacinacao_continuo",
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
        from {{ ref('raw_prontuario_vitacare_api__vacina') }}
    ),
    atendimentos_historico as (
        select 
            id_cnes,
            id_prontuario_global,
            profissional_nome,
            profissional_cbo,
            profissional_cns,
            profissional_cpf,
            patient_cpf
        from {{ ref('raw_prontuario_vitacare_api__acto') }}
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
            ut_id
        from {{ ref('raw_prontuario_vitacare_api__cadastro') }}
        qualify row_number() over(
            partition by ut_id, id_cnes
            order by greatest(data_cadastro, data_atualizacao_cadastro, updated_at) desc
        ) = 1
    ),

    agg_renamed as (
        select 
           v.id_vacinacao,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_vacinacao"
                    ]
                )
            }} as id_surrogate,
            v.id_cnes,
            p.codigo_equipe as id_equipe,
            p.ine_equipe as id_equipe_ine,
            p.microarea as id_microarea,
            p.npront as paciente_id_prontuario,
            p.cns as paciente_cns,
            p.cpf as paciente_cpf,
            e.estabelecimento_nome,
            p.equipe as equipe_nome,
            a.profissional_nome,
            a.profissional_cbo,
            a.profissional_cns,
            a.profissional_cpf,
            lower(v.nome_vacina) as vacina_descricao,
            case when v.dose = '1st Dose' then '1 dose'
                 when v.dose = '2nd Dose' then '2 dose'
                 when v.dose = '3rd Dose' then '3 dose'
                 when v.dose = '4rd Dose' then '4 dose'
                 when v.dose = '5ª Dose' then '5 dose'
                 when v.dose = '1st Booster' then '1 reforco'
                 when v.dose = '2st Booster' then '2 reforço'
                 when v.dose = '3st Booster' then '3 reforço'
                 when v.dose = 'Dose D' then 'dose d'
                 when v.dose = 'Single Dose' then 'dose unica'
                 when v.dose = 'Booster' then 'reforco'
                 when v.dose = 'Re-Vaccination' then 'revacinacao'
                else lower(v.dose) 
            end as vacina_dose,
            v.lote as vacina_lote,
            case when v.tipo_registro = 'Register of a past vaccine administration (Resgate)' then 'registro de aplicacao anterior'
                 when v.tipo_registro = 'Vaccine administration' then 'administracao'
                 when v.tipo_registro = 'Non Applicable' then 'nao aplicavel'
                else lower(v.tipo_registro) 
            end as vacina_registro_tipo,
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
            on a.patient_cpf = p.cpf
        left join estabelecimento e
            on v.id_cnes = e.id_cnes
    )

select * 
from agg_renamed