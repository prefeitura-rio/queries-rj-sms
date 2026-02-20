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

with source as (
    select * from {{source('brutos_siclom_api_staging', 'prep')}}
),

prep as (
    select 
        -- Controle 
        safe_cast(total as int64) as paginacao_total,
        safe_cast(RowNum as int64) as paginacao_linha,
        {{ process_null('cd_dis') }} as id_dispensacao,
        {{ process_null('num_sol') }} as id_solicitacao,

        -- Unidade Dispensadora
        {{ process_null('udm') }} as unidade_dispensadora,
        {{ process_null('municipio_udm') }} as unidade_dispensadora_municipio,
        {{ process_null('uf_udm') }} as unidade_dispensadora_uf,
        {{ process_null('tp_servico_atendimento') }} as servico_atendimento,
        {{ process_null('st_pub_priv') }} as esfera_atendimento,

        -- Identificação do Paciente
        {{ process_null('cpf') }} as paciente_cpf,
        {{ process_null('nome_civil') }} as paciente_nome,
        {{ process_null('nome_social') }} as paciente_nome_social,
        {{ process_null('ds_genero') }} as genero,

        -- Fatores de Risco
        {{ process_null('st_participante_estudo_vacina') }} as participante_estudo_vacina,
        {{ process_null('st_dinheiro_sexo') }} as dinheiro_sexo,
        {{ process_null('st_droga_injetavel') }} as droga_injetavel,
        {{ process_null('st_substancias_psicoativias') }} as substancias_psicoativas,

        -- Prescrição
        {{ process_null('tp_modalidade') }} as modalidade,
        {{ process_null('tp_esquema_prep') }} as esquema_prep,
        safe_cast(qtde_autoteste as int64) as autoteste_quantidade,
        safe_cast(duracao as int64) as duracao,
        safe_cast(dt_dispensa_sol as datetime) as dispensacao_solicitacao_data,
        
        cast(extracted_at as datetime) as extraido_em,
        date(data_particao) as data_particao
    from source
)

select * from prep