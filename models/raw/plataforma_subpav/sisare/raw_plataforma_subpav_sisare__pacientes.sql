{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'sisare__pacientes',
    materialized = 'table'
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_altas_referenciadas__pacientes') }}

),

base as (

    select
        id_paciente,
        id_sms_paciente,
        nome,
        cns,
        cpf,
        dt_nascimento,
        nome_mae,
        id_raca_cor,
        sexo,
        cep,
        logradouro,
        num_logradouro,
        complemento,
        bairro,
        municipio,
        uf,
        telefone,
        observacoes,
        unidade_referencia,
        status,
        created_at,
        updated_at,
        equipe_referencia,
        datalake_loaded_at
    from source

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_paciente)") }} as id_paciente,
        {{ normalize_null("regexp_replace(trim(id_sms_paciente), r'\\.0$', '')") }} as id_sms_paciente,
        {{ normalize_null("trim(nome)") }} as nome,
        {{ normalize_null("trim(cns)") }} as cns,
        {{ normalize_null("trim(cpf)") }} as cpf,
        {{ normalize_null("trim(dt_nascimento)") }} as dt_nascimento,
        {{ normalize_null("trim(nome_mae)") }} as nome_mae,
        {{ normalize_null("regexp_replace(trim(id_raca_cor), r'\\.0$', '')") }} as id_raca_cor,
        {{ normalize_null("trim(sexo)") }} as sexo,
        {{ normalize_null("trim(cep)") }} as cep,
        {{ normalize_null("trim(logradouro)") }} as logradouro,
        {{ normalize_null("trim(num_logradouro)") }} as num_logradouro,
        {{ normalize_null("trim(complemento)") }} as complemento,
        {{ normalize_null("trim(bairro)") }} as bairro,
        {{ normalize_null("trim(municipio)") }} as municipio,
        {{ normalize_null("trim(uf)") }} as uf,
        {{ normalize_null("trim(telefone)") }} as telefone,
        {{ normalize_null("trim(observacoes)") }} as observacoes,
        {{ normalize_null("regexp_replace(trim(unidade_referencia), r'\\.0$', '')") }} as unidade_referencia,
        {{ normalize_null("trim(status)") }} as status,
        {{ normalize_null("trim(created_at)") }} as created_at,
        {{ normalize_null("trim(updated_at)") }} as updated_at,
        {{ normalize_null("regexp_replace(trim(equipe_referencia), r'\\.0$', '')") }} as equipe_referencia,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by
            id_paciente,
            id_sms_paciente,
            nome,
            cns,
            cpf,
            dt_nascimento,
            nome_mae,
            id_raca_cor,
            sexo,
            cep,
            logradouro,
            num_logradouro,
            complemento,
            bairro,
            municipio,
            uf,
            telefone,
            observacoes,
            unidade_referencia,
            status,
            created_at,
            updated_at,
            equipe_referencia
        order by safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    id_paciente,
    safe_cast(id_sms_paciente as int64) as id_sms_paciente,
    nome,
    cns,
    cpf,
    safe.parse_date('%Y-%m-%d', dt_nascimento) as dt_nascimento,
    nome_mae,
    safe_cast(id_raca_cor as int64) as id_raca_cor,
    sexo,
    cep,
    logradouro,
    num_logradouro,
    complemento,
    bairro,
    municipio,
    uf,
    telefone,
    observacoes,
    safe_cast(unidade_referencia as int64) as unidade_referencia,
    safe_cast(status as int64) as status,
    safe.parse_datetime('%Y-%m-%d %H:%M:%S', created_at) as created_at,
    safe.parse_datetime('%Y-%m-%d %H:%M:%S', updated_at) as updated_at,
    safe_cast(equipe_referencia as int64) as equipe_referencia,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado