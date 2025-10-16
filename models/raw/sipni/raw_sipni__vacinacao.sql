{{
    config(
        alias="vacinacao",
        materialized="table",
    )
}}

with
    source as (
        select * from {{ source("brutos_sipni_staging", "vacinacao_pentavalente_rotina") }}
    ),

    casted as (
        select
            nu_cns_paciente,
            nu_cpf_paciente,
            no_paciente,
            TIMESTAMP_MICROS(CAST(dt_nascimento_paciente / 1000 AS INT64)) as dt_nascimento_paciente,
            nu_idade_paciente,
            tp_sexo_paciente,
            no_mae_paciente,
            no_pai_paciente,
            no_bairro_paciente,
            nu_cep_paciente,
            co_municipio_paciente,
            no_municipio_paciente,
            no_uf_paciente,
            co_pais_paciente,
            no_pais_paciente,
            co_cnes_estabelecimento,
            no_razao_social_estabelecimento,
            no_fantasia_estalecimento,
            co_municipio_estabelecimento,
            no_municipio_estabelecimento,
            no_uf_estabelecimento,
            ds_tipo_estabelecimento,
            ds_sub_tipo_estabelecimento,
            co_vacina,
            ds_vacina,
            sg_vacina,
            co_dose_vacina,
            ds_dose_vacina,
            TIMESTAMP_MICROS(CAST(dt_vacina / 1000 AS INT64)) as dt_vacina,
            co_lote_vacina,
            co_estrategia_vacinacao,
            ds_estrategia_vacinacao,
            co_vacina_fabricante,
            ds_vacina_fabricante,
            co_vacina_grupo_atendimento,
            ds_vacina_grupo_atendimento,
            co_vacina_categoria_atendimento,
            ds_vacina_categoria_atendimento
        from source
    )
select *
from casted