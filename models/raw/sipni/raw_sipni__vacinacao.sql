{{
    config(
        schema="brutos_sipni",
        alias="vacinacao", 
        materialized="table",
        unique_key = ['id_vacinacao'],
        cluster_by= ['id_cnes', 'vacina_nome'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with
    vacina as (
        select * from {{ source("brutos_sipni_staging", "vacinacao_historico") }}
    ),

    vacina_dedup as (
        select *
        from vacina
        qualify row_number() over (partition by co_identificador_sistema order by dt_entrada_datalake desc) = 1
    ),

    casted_renamed as (
        select
            cast({{ process_null('co_identificador_sistema') }} as string) as id_vacinacao,
            cast({{ process_null('co_cnes_estabelecimento') }} as string) as id_cnes,
            cast({{ process_null('nu_cns_paciente') }} as string) as paciente_cns,
            cast({{ process_null('nu_cpf_paciente') }} as string) as paciente_cpf,
            cast({{ process_null('no_paciente') }} as string) as paciente_nome,
            cast(dt_nascimento_paciente as date) as paciente_nascimento_data,
            case 
                when {{ process_null('tp_sexo_paciente') }} = 'M' then 'Masculino'
                when {{ process_null('tp_sexo_paciente') }} = 'F' then 'Feminino'
                else null
            end as paciente_sexo,
            cast({{ process_null('no_mae_paciente') }} as string) as paciente_nome_mae,
            cast({{ process_null('no_pai_paciente') }} as string) as paciente_nome_pai,
            cast({{ process_null('no_raca_cor_paciente') }} as string) as paciente_raca_cor,
            cast({{ process_null('nu_telefone_paciente') }} as string) as paciente_telefone,
            cast({{ process_null('ds_email_paciente') }} as string) as paciente_email,
            cast({{ process_null('no_uf_paciente') }} as string) as paciente_uf_nome,
            cast({{ process_null('ds_vacina') }} as string) as vacina_nome,
            cast({{ process_null('ds_dose_vacina') }} as string) as vacina_dose,
            cast(dt_vacina as date) as vacina_aplicacao_data,
            cast({{ process_null('co_lote_vacina') }} as string) as vacina_lote,
            cast({{ process_null('ds_estrategia_vacinacao') }} as string) as vacina_estrategia,
            cast({{ process_null('ds_local_aplicacao') }} as string) as vacina_local_aplicacao,
            cast({{ process_null('ds_origem_registro') }} as string) as vacina_tipo_registro,
            cast({{ process_null('co_profissional_vacina') }} as string) as profissional_codigo,
            cast({{ process_null('no_profissional_vacina') }} as string) as profissional_nome,
            cast(dt_entrada_datalake as datetime) as loaded_at,
            current_datetime('America/Sao_Paulo') as updated_at,
            cast(dt_vacina as date) as data_particao
        from vacina_dedup
    )

select *
from casted_renamed