{{
    config(
        alias="solicitacoes",
        materialized="table",
        partition_by={
            "field": "pedido_data_particao",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

with
    source as (
        select * from {{ source("brutos_biomega_staging", "solicitacoes") }}
    ),
    dedup as (
        select 
            * 
        from source
        qualify row_number() over (partition by id order by datalake_loaded_at desc) = 1
    ),
    renamed as (	
        select
            {{ process_null('id') }} as id_solicitacao,
            {{ process_null('link') }} as laudo_link,

            {{ process_null('codigoLis') }} as codigo_lis,
            {{ process_null('codigoApoio') }} as codigo_apoio,
            {{ process_null('codunidade') }} as codigo_unidade,
            {{ process_null('codorigem') }} as codigo_origem,
            {{ process_null('unidade') }} as unidade,
            {{ process_null('origem') }} as origem,
            safe_cast({{parse_datetime(process_null("dataPedido"))}} as datetime) as pedido_datahora,
            {{ process_null('status') }} as status,
            {{ process_null('mensagem') }} as mensagem,
            {{ process_null('alterado') }} as alterado,
            {{ process_null('autorizacao') }} as autorizacao,
            {{ process_null('responsaveltecnico_crm') }} as responsavel_tecnico_crm,
            {{ process_null('responsaveltecnico_nome') }} as responsavel_tecnico_nome,
            {{ process_null('paciente_codigoLis') }} as paciente_codigo_lis,
            {{ process_null('paciente_nome') }} as paciente_nome,
            {{ process_null('paciente_nascimento') }} as paciente_nascimento_data,
            case 
                when {{process_null("paciente_sexo")}} = 'M' then 'male'
                when {{process_null("paciente_sexo")}} = 'F' then 'female'
                else null
            end as paciente_sexo,
            {{ process_null('paciente_cpf') }} as paciente_cpf,
            {{ process_null('paciente_codsus') }} as paciente_cns,
            {{ process_null('paciente_nomeMae') }} as paciente_nome_mae,

            {{ process_null('lote_identificadorLis') }} as lote_identificador_lis,
            {{ process_null('lote_criacaoLis') }} as lote_criacao_lis,
            {{ process_null('lote_criacaoApoio') }} as lote_criacao_apoio,
            {{ process_null('lote_codigoLis') }} as lote_codigo_lis,
            {{ process_null('lote_origemLis') }} as lote_origem_lis,

            safe_cast({{ process_null('datalake_loaded_at') }} as timestamp) as loaded_at,
            safe_cast({{parse_datetime(process_null("dataPedido"))}} as date) as pedido_data_particao
        from dedup
    )
select *
from renamed