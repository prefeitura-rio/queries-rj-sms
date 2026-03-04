{{
    config(
        alias="vacinacao",
        materialized="table",
    )
}}

with
    source as (
        select * from {{ source("brutos_sipni_staging", "vacinacao_historico") }}
    ),

    casted_renamed as (
        select
            safe_cast({{ process_null('co_identificador_sistema') }} as string) as id_vacinacao,
            safe_cast({{ process_null('co_cnes_estabelecimento') }} as string) as id_cnes,
            safe_cast({{ process_null('nu_cns_paciente') }} as string) as paciente_cns,
            safe_cast({{ process_null('nu_cpf_paciente') }} as string) as paciente_cpf,
            safe_cast({{ process_null('no_paciente') }} as string) as paciente_nome,
            safe_cast(dt_nascimento_paciente as date) as paciente_nascimento_data,
            case 
                when {{ process_null('tp_sexo_paciente') }} = 'M' then 'Masculino'
                when {{ process_null('tp_sexo_paciente') }} = 'F' then 'Feminino'
                else null
            end as paciente_sexo,
            safe_cast({{ process_null('no_mae_paciente') }} as string) as paciente_nome_mae,
            safe_cast({{ process_null('no_pai_paciente') }} as string) as paciente_nome_pai,
            safe_cast({{ process_null('no_raca_cor_paciente') }} as string) as paciente_raca_cor,
            safe_cast({{ process_null('nu_telefone_paciente') }} as string) as paciente_telefone,
            safe_cast({{ process_null('ds_email_paciente') }} as string) as paciente_email,
            safe_cast({{ process_null('no_uf_paciente') }} as string) as paciente_uf_nome,
            safe_cast({{ process_null('ds_vacina') }} as string) as vacina_nome,
            safe_cast({{ process_null('ds_dose_vacina') }} as string) as vacina_dose,
            safe_cast(dt_vacina as date) as vacina_aplicacao_data,
            safe_cast({{ process_null('co_lote_vacina') }} as string) as vacina_lote,
            safe_cast({{ process_null('ds_estrategia_vacinacao') }} as string) as vacina_estrategia,
            safe_cast({{ process_null('ds_local_aplicacao') }} as string) as vacina_local_aplicacao,
            safe_cast({{ process_null('ds_origem_registro') }} as string) as vacina_tipo_registro,
            safe_cast({{ process_null('co_profissional_vacina') }} as string) as profissional_codigo,
            safe_cast({{ process_null('no_profissional_vacina') }} as string) as profissional_nome,
            safe_cast(dt_entrada_datalake as datetime) as loaded_at
        from source
    )

select *
from casted_renamed