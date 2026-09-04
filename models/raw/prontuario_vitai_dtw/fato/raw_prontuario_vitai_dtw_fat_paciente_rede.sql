{{
    config(
        alias="fat_paciente_rede",
        materialized="incremental",
        unique_key="fat_paciente_rede_id"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_paciente_rede") }}
),

renomeado as (
    select
        safe_cast(tipologradouro as string) as tipologradouro,
        safe_cast(data_nascimento as date) as data_nascimento,
        safe_cast(nome as string) as nome,
        safe_cast(nome_alternativo as string) as nome_alternativo,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(transex as string) as transex,
        safe_cast(nomemae as string) as nomemae,
        safe_cast(mun_id as int64) as mun_id,
        safe_cast(cep as string) as cep,
        safe_cast(nomepai as string) as nomepai,
        safe_cast(bai_id as int64) as bai_id,
        safe_cast(telefone_extra_dois as string) as telefone_extra_dois,
        safe_cast(cns as string) as cns,
        safe_cast(naturalidade as string) as naturalidade,
        safe_cast(email as string) as email,
        safe_cast(nomelogradouro as string) as nomelogradouro,
        safe_cast(celular as string) as celular,
        safe_cast(sexo as string) as sexo,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(cpf as string) as cpf,
        safe_cast(telefone_extra_um as string) as telefone_extra_um,
        safe_cast(raca_id as int64) as raca_id,
        safe_cast(telefone as string) as telefone,
        safe_cast(datahora as datetime) as datahora,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
