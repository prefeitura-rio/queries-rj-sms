{{
    config(
        alias="exames", 
    )
}}

with
    source as (
        select 
            * 
        from {{ source("brutos_exames_laboratoriais_staging", "exames") }}
    ),

    dedup as (
        select 
            * 
        from source
        qualify row_number() over (partition by id order by datalake_loaded_at desc) = 1
    ),

    renamed as (
        select
            safe_cast({{process_null("id")}} as string) as id,
            safe_cast({{process_null("solicitacao_id")}} as string) as id_solicitacao,

            safe_cast({{process_null("codigoLis")}} as string) as codigo_lis,
            safe_cast({{process_null("codigoApoio")}} as string) as codigo_apoio,
            safe_cast({{process_null("codigoExame")}} as string) as codigo_exame,
            safe_cast({{process_null("amostraLis")}} as string) as amostra_lis,
            safe_cast({{process_null("amostraApoio")}} as string) as amostra_apoio,
            safe_cast({{process_null("materialLis")}} as string) as material_lis,
            safe_cast({{process_null("sequenciaLis")}} as string) as sequencia_lis,
            safe_cast({{process_null("examedependenteLis")}} as string) as examedependente_lis,
            safe_cast({{process_null("amostraApoioAnterior")}} as string) as amostra_apoio_anterior,
            safe_cast({{process_null("parcial")}} as string) as parcial,
            safe_cast({{process_null("retificado")}} as string) as retificado,


            safe_cast({{process_null("usuarioAssinatura")}} as string) as usuario_assinatura,
            safe_cast({{process_null("usuarioAssinaturaCpf")}} as string) as usuario_assinatura_cpf,
            safe_cast({{parse_datetime(process_null("dataAssinatura"))}} as datetime) as data_assinatura,

            safe_cast({{process_null("alterado")}} as string) as alterado,
            safe_cast({{process_null("status")}} as string) as status,
            safe_cast({{process_null("mensagem")}} as string) as mensagem,
            safe_cast({{process_null("impresso")}} as string) as impresso,
            safe_cast({{parse_datetime(process_null("codigoVersao"))}} as datetime) as codigo_versao,
            safe_cast({{process_null("descricaoMetodo")}} as string) as descricao_metodo,

            safe_cast({{process_null("solicitante_numero")}} as string) as solicitante_numero,
            safe_cast({{process_null("solicitante_conselho")}} as string) as solicitante_conselho,
            safe_cast({{process_null("solicitante_uf")}} as string) as solicitante_uf,
            safe_cast({{process_null("solicitante_nome")}} as string) as solicitante_nome,

            safe_cast({{process_null("source")}} as string) as origem,
            safe_cast({{process_null("datalake_loaded_at")}} as datetime) as datalake_loaded_at
        from dedup
    )

select 
    *
from renamed