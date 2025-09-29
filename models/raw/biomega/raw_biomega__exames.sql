{{
    config(
        alias="exames",
        materialized="table",
        partition_by={
            "field": "assinatura_data_particao",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

with
    source as (
        select * from {{ source("brutos_biomega_staging", "exames") }}
    ),
    dedup as (
        select 
            * 
        from source
        qualify row_number() over (partition by id order by datalake_loaded_at desc) = 1
    ),
    renamed as (	
        select
            {{ process_null('id') }} as id_exame,
            {{ process_null('solicitacao_id') }} as id_solicitacao,

            {{ process_null('codigoLis') }} as codigo_lis,
            {{ process_null('codigoApoio') }} as codigo_apoio,
            {{ process_null('codigoExame') }} as codigo_exame,
            {{ process_null('AmostraLis') }} as amostra_lis,
            {{ process_null('amostraApoio') }} as amostra_apoio,
            {{ process_null('materialLis') }} as material_lis,
            {{ process_null('sequenciaLis') }} as sequencia_lis,
            {{ process_null('examedependenteLis') }} as exame_dependente_lis,
            {{ process_null('parcial') }} as parcial,
            {{ process_null('retificado') }} as retificado,
            {{ process_null('usuarioAssinatura') }} as usuario_assinatura,
            {{ process_null('usuarioAssinaturaCpf') }} as usuario_assinatura_cpf,
            safe_cast({{parse_datetime(process_null("dataAssinatura"))}} as datetime) as assinatura_datahora,
            {{ process_null('alterado') }} as alterado,
            {{ process_null('status') }} as status,
            {{ process_null('mensagem') }} as mensagem,
            {{ process_null('impresso') }} as impresso,
            safe_cast({{parse_datetime(process_null("codigoVersao"))}} as datetime) as codigo_versao_data,
            {{ process_null('descricaoMetodo') }} as descricao_metodo,
            {{ process_null('solicitante_nome') }} as solicitante_nome,
            {{ process_null('solicitante_numero') }} as solicitante_numero,
            {{ process_null('solicitante_conselho') }} as solicitante_conselho,
            {{ process_null('solicitante_uf') }} as solicitante_uf,

            safe_cast({{ process_null('datalake_loaded_at') }} as timestamp) as loaded_at,
            safe_cast({{parse_datetime(process_null("dataAssinatura"))}} as date) as assinatura_data_particao
        from dedup
    )
select *
from renamed