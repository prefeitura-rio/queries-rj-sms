{{
    config(
        alias="solicitacoes",  
    )
}}

with
    source as (
        select * from {{ source("brutos_cientificalab_staging", "solicitacoes") }}
    ),
    removendo_duplicados as (
        select * from source
        qualify
            row_number() over (partition by id order by datalake_loaded_at desc) = 1
    ),
    renamed as (	
        select
            safe_cast({{process_null("id")}} as string) as id,
            safe_cast({{process_null("link")}} as string) as laudo_url,
            safe_cast({{process_null("codigoLis")}} as string) as cod_lis,
            safe_cast({{process_null("codigoApoio")}} as string) as cod_apoio,
            safe_cast({{process_null("codunidade")}} as string) as cod_unidade,
            safe_cast({{process_null("unidade")}} as string) as unidade,
            safe_cast({{process_null("codorigem")}} as string) as cod_origem,
            safe_cast({{process_null("origem")}} as string) as origem,
            safe_cast({{parse_datetime(process_null("dataPedido"))}} as timestamp) as datahora_pedido,
            safe_cast({{process_null("autorizacao")}} as string) as autorizacao,
            safe_cast({{process_null("status")}} as string) as status,
            safe_cast({{process_null("mensagem")}} as string) as mensagem,
            safe_cast({{process_null("alterado")}} as string) as alterado,
            safe_cast({{process_null("responsaveltecnico_crm")}} as string) as responsaveltecnico_crm,
            safe_cast({{process_null("responsaveltecnico_nome")}} as string) as responsaveltecnico_nome,

            safe_cast({{process_null("paciente_codigoLis")}} as string) as paciente_cod_lis,
            safe_cast({{process_null("paciente_nome")}} as string) as paciente_nome,
            safe_cast({{parse_date(process_null("paciente_nascimento"))}} as date) as paciente_nascimento,
            CASE 
                WHEN {{process_null("paciente_sexo")}} = 'M' THEN 'male'
                WHEN {{process_null("paciente_sexo")}} = 'F' THEN 'female'
                ELSE null
            END as paciente_sexo,
            safe_cast({{process_null("paciente_cpf")}} as string) as paciente_cpf,
            safe_cast({{process_null("paciente_codsus")}} as string) as paciente_cns,
            safe_cast({{process_null("paciente_nomeMae")}} as string) as paciente_nome_mae,

            safe_cast({{process_null("lote_identificadorLis")}} as string) as lote_identificador_lis,
            safe_cast({{process_null("lote_criacaoLis")}} as string) as lote_criacao_lis,
            safe_cast({{process_null("lote_criacaoApoio")}} as timestamp) as lote_criacao_apoio,
            safe_cast({{process_null("lote_codigoLis")}} as string) as lote_codigo_lis,
            safe_cast({{process_null("lote_origemLis")}} as string) as lote_origem_lis,

            safe_cast({{process_null("datalake_loaded_at")}} as timestamp) as loaded_at,
            safe_cast(current_timestamp() as timestamp) as processed_at
        from removendo_duplicados
    )
select *
from renamed