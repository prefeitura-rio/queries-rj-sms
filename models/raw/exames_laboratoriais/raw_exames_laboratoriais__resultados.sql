{{
    config(
        alias="resultados",
    )
}}

with
    source as (
        select * from {{ source("brutos_exames_laboratoriais_staging", "resultados") }}
    ),
    removendo_duplicados as (
        select * from source
        qualify
            row_number() over (partition by id order by datalake_loaded_at desc) = 1
    ),
    renamed as (
        select
            safe_cast({{process_null("id")}} as string) as id,
            safe_cast({{process_null("exame_id")}} as string) as id_exame,

            safe_cast({{process_null("codigoLis")}} as string) as codigo_lis,
            safe_cast({{process_null("codigoApoio")}} as string) as codigo_apoio,
            safe_cast({{process_null("descricaoApoio")}} as string) as descricao_apoio,

            safe_cast({{process_null("decimal")}} as string) as decimal,
            safe_cast({{process_null("tipo")}} as string) as tipo,
            safe_cast({{process_null("resultado")}} as string) as resultado,
            safe_cast({{process_null("unidade")}} as string) as unidade,
            CASE 
                WHEN {{process_null("alterado")}} = 'S' THEN 'sim'
                WHEN {{process_null("alterado")}} = 'N' THEN 'nao'
                ELSE null
            END as alterado,
            safe_cast({{process_null("valorReferenciaTexto")}} as string) as valor_referencia_texto,
            safe_cast({{process_null("valorReferenciaMinimo")}} as string) as valor_referencia_minimo,
            safe_cast({{process_null("valorReferenciaMaximo")}} as string) as valor_referencia_maximo,

            safe_cast({{process_null("source")}} as string) as origem,
            safe_cast({{process_null("datalake_loaded_at")}} as datetime) as loaded_at
        from removendo_duplicados
    )
select *
from renamed