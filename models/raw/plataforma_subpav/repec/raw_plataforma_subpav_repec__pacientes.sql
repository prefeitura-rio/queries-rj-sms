{{
    config(
        alias="repec__pacientes",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__pacientes") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_paciente') }} as id_paciente,
            {{ process_null('num_prontuario') }} as num_prontuario,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('nome_paciente') ~ " as string)") }}) as nome_paciente,
            {{ process_null('data_nasc_paciente') }} as data_nasc_paciente,
            {{ process_null('sexo_paciente') }} as sexo_paciente,
            {{ process_null('sexo_informado') }} as sexo_informado,
            {{ process_null('cpf_paciente') }} as cpf_paciente,
            {{ process_null('dnv_paciente') }} as dnv_paciente,
            {{ process_null('num_sus_paciente') }} as num_sus_paciente,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('nome_social_paciente') ~ " as string)") }}) as nome_social_paciente,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('nome_mae_paciente') ~ " as string)") }}) as nome_mae_paciente,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('nome_pai_paciente') ~ " as string)") }}) as nome_pai_paciente,
            {{ process_null('raca_cor_paciente') }} as raca_cor_paciente,
            {{ process_null('nacionalidade_paciente') }} as nacionalidade_paciente,
            {{ process_null('municipio_nasc_paciente') }} as municipio_nasc_paciente,
            {{ process_null('pais_nasc_paciente') }} as pais_nasc_paciente,
            {{ process_null('num_passaporte_paciente') }} as num_passaporte_paciente,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('cep_endereco') ~ " as string)") }}) as cep_endereco,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('tipo_logradouro_endereco') ~ " as string)") }}) as tipo_logradouro_endereco,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('endereco') ~ " as string)") }}) as endereco,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('num_endereco') ~ " as string)") }}) as num_endereco,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('bairro_endereco') ~ " as string)") }}) as bairro_endereco,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('municipio_endereco') ~ " as string)") }}) as municipio_endereco,
            {{ process_null('frequenta_escola') }} as frequenta_escola,
            {{ process_null('escolaridade_paciente') }} as escolaridade_paciente,
            {{ process_null('possui_plano_saude') }} as possui_plano_saude,
            {{ process_null('data_registro_paciente') }} as data_registro_paciente,
            {{ process_null('data_obito_paciente') }} as data_obito_paciente,
            {{ process_null('observacoes_pacientes') }} as observacoes_pacientes,
            {{ process_null('origem_arquivo') }} as origem_arquivo,
            {{ process_null('origem_banco') }} as origem_banco,
            {{ repec_origem_unidade_para_cnes("origem_unidade") }} as cnes_origem,
            safe_cast({{ process_null('datalake_loaded_at') }} as timestamp) as datalake_loaded_at
        from source
    ),

    deduplicar as (
        select *
        from tratar_campos
        qualify row_number() over (
            partition by
                    id_paciente,
                    num_prontuario,
                    nome_paciente,
                    data_nasc_paciente,
                    sexo_paciente,
                    sexo_informado,
                    cpf_paciente,
                    dnv_paciente,
                    num_sus_paciente,
                    nome_social_paciente,
                    nome_mae_paciente,
                    nome_pai_paciente,
                    raca_cor_paciente,
                    nacionalidade_paciente,
                    municipio_nasc_paciente,
                    pais_nasc_paciente,
                    num_passaporte_paciente,
                    cep_endereco,
                    tipo_logradouro_endereco,
                    endereco,
                    num_endereco,
                    bairro_endereco,
                    municipio_endereco,
                    frequenta_escola,
                    escolaridade_paciente,
                    possui_plano_saude,
                    data_registro_paciente,
                    data_obito_paciente,
                    observacoes_pacientes
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
