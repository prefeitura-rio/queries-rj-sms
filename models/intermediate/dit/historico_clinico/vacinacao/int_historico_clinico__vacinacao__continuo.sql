{{
    config(
        alias="vacinacao_continuo",
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
        from {{ ref('raw_prontuario_vitacare_api__vacina') }}
    ),

    atendimento as (
        select 
            * 
        from {{ ref('raw_prontuario_vitacare_api__acto') }}
    ),

    paciente as (
        select 
            * 
        from {{ ref('raw_prontuario_vitacare_api__cadastro') }}
        qualify row_number() over( partition by ut_id, id_cnes order by greatest(data_cadastro, data_atualizacao_cadastro, updated_at) desc
        ) = 1
    ),

    casted_normalized as (
        select 
            v.id_vacinacao,
            v.id_cnes,
            p.codigo_equipe as id_equipe,
            p.ine_equipe as id_ine_equipe,
            p.microarea as id_microarea,
            p.npront as paciente_id_prontuario,
            p.cns as paciente_cns,
            p.cpf as paciente_cpf,
            e.nome_limpo as estabelecimento_nome,
            lower(p.equipe) as equipe_nome,
            {{ proper_br('a.profissional_nome')}} as profissional_nome,
            a.profissional_cbo,
            a.profissional_cns,
            a.profissional_cpf,
            lower(v.nome_vacina) as vacina_descricao,
            case 
                when v.dose = '1st Dose' then '1 dose'
                when v.dose = '2nd Dose' then '2 dose'
                when v.dose = '3rd Dose' then '3 dose'
                when v.dose = '4th Dose' then '4 dose'
                when v.dose = '5th Dose' then '5 dose'
                when v.dose = '1st Booster' then '1 reforco'
                when v.dose = '2nd Booster' then '2 reforco'
                when v.dose = '3rd Booster' then '3 reforco'
                when v.dose = 'Dose D' then 'dose d'
                when v.dose = 'Single Dose' then 'dose unica'
                when v.dose = 'Booster' then 'reforco'
                when v.dose = 'Re-Vaccination' then 'revacinacao'
                else lower(v.dose) 
            end as vacina_dose,
            v.lote as vacina_lote,
            case 
                when v.tipo_registro = 'Register of a past vaccine administration (Resgate)' then 'registro de vacinacao anterior'
                when v.tipo_registro = 'Vaccine administration' then 'administracao'
                when v.tipo_registro = 'Non Applicable' then 'nao aplicada'
                else lower(v.tipo_registro) 
            end as vacina_registro_tipo,
            lower(v.estrategia_imunizacao) as vacina_estrategia,
            v.diff as vacina_diff,
            v.data_aplicacao  as vacina_aplicacao_data,
            safe_cast(v.data_registro as date) as vacina_registro_data,
            {{ proper_br('p.nome')}} as paciente_nome,
            lower(p.sexo) as paciente_sexo,
            p.data_nascimento as paciente_nascimento_data,
            p.nome_mae as paciente_nome_mae,
            safe_cast(null as date) as paciente_mae_nascimento_data,
            p.situacao_usuario as paciente_situacao,
            safe_cast(p.data_cadastro as date) as paciente_cadastro_data,
            p.obito as paciente_obito,
            safe_cast(v.loaded_at as datetime) as loaded_at,
            safe_cast(v.data_registro as date) as particao_registro_vacinacao
        from vacina v
        left join atendimento a
            on v.id_prontuario_global = a.id_prontuario_global
        left join paciente p
            on a.ut_id = p.ut_id
            and a.id_cnes = p.id_cnes
        left join estabelecimento e
            on v.id_cnes = e.id_cnes
    )

select 
    *
from casted_normalized






