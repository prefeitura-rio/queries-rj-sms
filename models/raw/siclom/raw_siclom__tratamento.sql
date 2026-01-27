{{
    config(
        schema="brutos_siclom_api",
        alias="tratamento",
        tags=["siclom"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}

with 
    source as (select * from {{ source('brutos_siclom_api_staging', 'tratamento') }})

select
    {{ process_null('udm') }} as unidade_nome,
    {{ process_null('municipio_udm') }} as unidade_municipio,
    {{ process_null('uf_udm') }} as unidade_uf,
    {{ process_null('CPF') }} as cpf,
    {{ process_null('nome_civil') }} as paciente_nome,
    {{ process_null('nome_social') }} as paciente_nome_social,
    {{ process_null('ds_genero') }} as genero,
    {{ process_null('data_dispensa') }} as data_dispensa,
    {{ process_null('categoria_usuario') }} as categoria_usuario,
    {{ process_null('idade_gestacional') }} as idade_gestacional,
    {{ process_null('dt_parto') }} as parto_data,
    {{ process_null('tp_servico_atendimento') }} as tipo_servico_atendimento,
    {{ process_null('st_pub_priv') }} as tipo_publico_privado,
    {{ process_null('mudanca_tratamento') }} as mudanca_tratamento,
    {{ process_null('coinfectado') }} as coinfectado,
    {{ process_null('uf_crm') }} as crm_uf,
    {{ process_null('cd_crm') }} as codigo_crm,
    {{ process_null('nome_medico') }} as medico_nome,
    {{ process_null('esquema') }} as esquema,
    {{ process_null('duracao') }} as duracao,
    {{ process_null('extracted_at') }} as extraido_em,
    cast(cpf as int64) as cpf_particao
from source