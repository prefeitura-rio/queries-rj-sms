{{
    config(
        alias="bolsista",
        materialized="table",
        unique_key="id",
    )
}}

with
    source as (
        select * from {{ source("brutos_seguir_em_frente_staging", "bolsista") }}
    ),
    renamed as (
        select
            {{ adapter.quote("cpf") }},
            {{ adapter.quote("nome") }} as nome,
            {{ adapter.quote("data_de_nascimento") }} as data_nascimento,
            {{ adapter.quote("observacoes") }},
            {{ adapter.quote("termo_de_compromisso_assinado") }}
            as termo_de_compromisso_url,
            {{ adapter.quote("foto_do_rg") }} as rg_url,
            {{ adapter.quote("ja_possui_conta_corrente_no_santander") }}
            as santander_conta_indicador,
            {{ adapter.quote("agencia_santander") }} as santander_agencia_numero,
            {{ adapter.quote("conta_santander") }} as santander_conta_numero,
            {{ adapter.quote("identificador_pcsm") }} as id_pcsm,
            {{ adapter.quote("caps_de_referencia") }} as saude_mental_estabelecimento,
            {{ adapter.quote("fase_atual") }} as fase_atual,
            {{ adapter.quote("fase_1___data_de_inicio") }} fase_1_data_inicio,
            {{ adapter.quote("fase_1___unidade_de_acolhimento") }}
            as fase_1_estabelecimento,
            {{ adapter.quote("fase_1___monitor") }} as fase_1_monitor,
            {{ adapter.quote("fase_2___data_de_inicio_da_fase") }}
            as fase_2_data_inicio,
            {{ adapter.quote("fase_2___unidade_de_saude") }} as fase_2_estabelecimento,
            {{ adapter.quote("created_at") }} as criado_em,
            {{ adapter.quote("creator") }} as criado_por,
            {{ adapter.quote("id") }},
            {{ adapter.quote("ano_particao") }},
            {{ adapter.quote("mes_particao") }},
            {{ adapter.quote("data_particao") }}

        from source
    )
select
    -- pk
    id,

    -- fk
    utils.clean_numeric_string(cpf) as cpf,
    id_pcsm,

    -- common fields
    upper(nome) as name,
    safe_cast(data_nascimento as date) as data_nascimento,
    saude_mental_estabelecimento,
    rg_url,
    termo_de_compromisso_url,
    lower(
        utils.clean_name_string(santander_conta_indicador)
    ) as santander_conta_indicador,
    santander_agencia_numero,
    santander_conta_numero,
    upper(observacoes) as observacoes,
    fase_atual,
    safe_cast(fase_1_data_inicio as date) as fase_1_data_inicio,
    fase_1_estabelecimento,
    fase_1_monitor,
    safe_cast(fase_2_data_inicio as date) as fase_2_data_inicio,
    fase_2_estabelecimento,

    -- metadata
    safe_cast(criado_em as datetime) as criado_em,
    criado_por,
    ano_particao,
    mes_particao,
    safe_cast(data_particao as date) as data_particao

from renamed
