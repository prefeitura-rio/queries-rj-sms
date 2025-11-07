{{
    config(
        alias="exames", 
    )
}}

with

    source as (
        select * from {{ source("brutos_cientificalab_staging", "exames") }}
    ),
    removendo_duplicados as (
        select * from source
        qualify
            row_number() over (partition by id order by datalake_loaded_at desc) = 1
    ),
    renamed as (
        select
            safe_cast({{process_null("id")}} as string) as id,
            safe_cast({{process_null("solicitacao_id")}} as string) as solicitacao_id,

            safe_cast({{process_null("codigoLis")}} as string) as cod_lis,
            safe_cast({{process_null("amostraLis")}} as string) as amostra_lis,
            safe_cast({{process_null("codigoApoio")}} as string) as cod_apoio,
            safe_cast({{process_null("codigoExame")}} as string) as cod_exame,
            safe_cast({{process_null("amostraApoio")}} as string) as amostra_apoio,
            safe_cast({{process_null("sequenciaLis")}} as string) as sequencia_lis,
            safe_cast({{process_null("examedependenteLis")}} as string) as examedependente_lis,
            safe_cast({{process_null("amostraApoioAnterior")}} as string) as amostra_apoio_anterior,
            safe_cast({{process_null("parcial")}} as string) as parcial,
            safe_cast({{process_null("retificado")}} as string) as retificado,


            safe_cast({{process_null("usuarioAssinatura")}} as string) as usuario_assinatura,
            safe_cast({{process_null("usuarioAssinaturaCpf")}} as string) as usuario_assinatura_cpf,
            safe_cast({{process_null("dataAssinatura")}} as string) as data_assinatura,

            safe_cast({{process_null("alterado")}} as string) as alterado,
            safe_cast({{process_null("status")}} as string) as status,
            safe_cast({{process_null("mensagem")}} as string) as mensagem,

            safe_cast({{process_null("solicitante_numero")}} as string) as solicitante_numero,
            safe_cast({{process_null("solicitante_conselho")}} as string) as solicitante_conselho,
            safe_cast({{process_null("solicitante_uf")}} as string) as solicitante_uf,
            safe_cast({{process_null("solicitante_nome")}} as string) as solicitante_nome,

            safe_cast({{process_null("datalake_loaded_at")}} as timestamp) as datalake_loaded_at,
            safe_cast(current_timestamp() as timestamp) as processed_at
        from removendo_duplicados
    )
select *
from renamed