{{
    config(
        schema="brutos_siclom_api",
        alias="prep",
        tags=["siclom"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

-- TODO: Confirmar nome das colunas com a SAP

with source as (
    select * from {{source('brutos_siclom_api_staging', 'prep')}}
),

prep as (
    select 
    {{ process_null('CPF') }} as cpf,
    {{ process_null('cd_dis') }} as cd_dis,
    {{ process_null('num_sol') }} as id_solicitacao,
    {{ process_null('udm') }} as unidade,
    {{ process_null('municipio_udm') }} as municipio,
    {{ process_null('uf_udm') }} as uf, 
    {{ process_null('tp_servico_atendimento') }} as servico_atendimento,
    {{ process_null('st_pub_priv') }} as esfera_atendimento,
    {{ process_null('nome_civil') }} as nome_civil,
    {{ process_null('nome_social') }} as nome_social,
    {{ process_null('ds_genero') }} as genero,
    {{ process_null('st_participante_estudo_vacina') }} as participante_estudo_vacina,
    {{ process_null('st_dinheiro_sexo') }} as dinheiro_sexo,
    {{ process_null('st_droga_injetavel') }} as droga_injetavel,
    {{ process_null('st_substancias_psicoativias') }} as substancias_psicoativas,
    {{ process_null('tp_modalidade') }} as modalidade,
    {{ process_null('tp_esquema_prep') }} as esquema_prep,
    {{ process_null('qtde_autoteste') }} as quantidade_autoteste,
    {{ process_null('duracao') }} as duracao,
    {{ process_null('dt_dispensa_sol') }} as solicitacao_dispensa_data, 
    {{ process_null('extracted_at') }} as extraido_em,
    date(data_particao) as data_particao
    from source
)

select * from prep