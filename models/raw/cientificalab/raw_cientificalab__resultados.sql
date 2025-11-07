{{
    config(
        alias="resultados",
    )
}}

with
    source as (
        select * from {{ source("brutos_cientificalab_staging", "resultados") }}
    ),
    removendo_duplicados as (
        select * from source
        qualify
            row_number() over (partition by id order by datalake_loaded_at desc) = 1
    ),
    renamed as (
        select
            safe_cast({{process_null("id")}} as string) as id,
            safe_cast({{process_null("exame_id")}} as string) as exame_id,

            safe_cast({{process_null("codigoLis")}} as string) as cod_lis,
            safe_cast({{process_null("codigoApoio")}} as string) as cod_apoio,
            safe_cast({{process_null("descricaoApoio")}} as string) as descricao_apoio,

            safe_cast({{process_null("decimal")}} as integer) as decimal,
            safe_cast({{process_null("tipo")}} as string) as tipo,
            safe_cast(replace({{process_null("resultado")}}, ',', '.') as float64) as resultado,
            safe_cast({{process_null("unidade")}} as string) as unidade,
            CASE 
                WHEN {{process_null("alterado")}} = 'S' THEN 'sim'
                WHEN {{process_null("alterado")}} = 'N' THEN 'nao'
                ELSE null
            END as alterado,
            safe_cast(replace({{process_null("valorReferenciaTexto")}}, ',', '.') as float64) as valor_referencia_texto,
            safe_cast(replace({{process_null("valorReferenciaMinimo")}}, ',', '.') as float64) as valor_referencia_minimo,
            safe_cast(replace({{process_null("valorReferenciaMaximo")}}, ',', '.') as float64) as valor_referencia_maximo,

            safe_cast({{process_null("datalake_loaded_at")}} as timestamp) as loaded_at,
            safe_cast(current_timestamp() as timestamp) as processed_at
        from removendo_duplicados
    )
select *
from renamed