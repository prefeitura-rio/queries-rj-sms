{{
    config(
        schema="brutos_siclom_api",
        alias="tratamento",
        tags=["siclom"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

-- TODO: Confirmar nome das colunas com a SAP

with 
    source as (select * from {{ source('brutos_siclom_api_staging', 'tratamento') }})

select
    -- Unidade Dispensadora 
    {{ process_null('udm') }} as unidade_dispensadora,
    {{ process_null('municipio_udm') }} as unidade_dispensadora_municipio,
    {{ process_null('uf_udm') }} as unidade_dispensadora_uf,
    {{ process_null('tp_servico_atendimento') }} as servico_atendimento,
    {{ process_null('st_pub_priv') }} as esfera_atendimento,

    -- Identificação do paciente
    {{ process_null('CPF') }} as paciente_cpf,
    {{ process_null('nome_civil') }} as paciente_nome,
    {{ process_null('nome_social') }} as paciente_nome_social,
    {{ process_null('ds_genero') }} as paciente_genero,

    -- Dados clínicos
    {{ process_null('categoria_usuario') }} as categoria_usuario,
    safe_cast(idade_gestacional as int64) as idade_gestacional,
    safe_cast(dt_parto as date) as parto_data,
    {{ process_null('coinfectado') }} as coinfectado,

    -- Prescrição
    safe_cast(data_dispensa as datetime) as dispensa_data,
    {{ process_null('mudanca_tratamento') }} as mudanca_tratamento,
    {{ process_null('esquema') }} as esquema,
    {{ process_null('duracao') }} as duracao,

    -- Médico prescritor
    {{ process_null('nome_medico') }} as medico_nome,
    {{ process_null('cd_crm') }} as id_conselho,
    {{ process_null('uf_crm') }} as conselho_uf,

    cast(extracted_at as datetime) as extraido_em,
    date(data_particao) as data_particao
from source