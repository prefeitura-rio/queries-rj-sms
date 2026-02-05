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

    casted as (
        select
            safe_cast(co_identificador_sistema as string) as id_vacinacao,
            safe_cast(co_cnes_estabelecimento as string) as id_cnes,
            safe_cast(nu_cns_paciente as string) as paciente_cns,
            safe_cast(nu_cpf_paciente as string) as paciente_cpf,
            safe_cast(no_paciente as string) as paciente_nome,
            safe_cast(dt_nascimento_paciente as date) as paciente_nascimento_data,
            case when tp_sexo_paciente = 'M' then 'Masculino'
                 when tp_sexo_paciente = 'F' then 'Feminino'
                 else null
            end as paciente_sexo,
            safe_cast(no_mae_paciente as string) as paciente_nome_mae,
            safe_cast(no_pai_paciente as string) as paciente_nome_pai,
            safe_cast(no_raca_cor_paciente as string) as paciente_raca_cor,
            safe_cast(nu_telefone_paciente as string) as paciente_telefone,
            safe_cast(ds_email_paciente as string) as paciente_email,
            safe_cast(no_uf_paciente as string) as paciente_uf_nome,
            safe_cast(ds_vacina as string) as vacina_nome,
            safe_cast(ds_dose_vacina as string) as vacina_dose,
            safe_cast(dt_vacina as string) as vacina_data,
            safe_cast(co_lote_vacina as string) as vacina_lote,
            safe_cast(ds_estrategia_vacinacao as string) as vacina_estrategia,
            safe_cast(ds_local_aplicacao as string) as vacina_local_aplicacao,
            safe_cast(ds_origem_registro as string) as vacina_tipo_registro,
            safe_cast(co_profissional_vacina as string) as profissional_codigo,
        from source
    )
select *
from casted