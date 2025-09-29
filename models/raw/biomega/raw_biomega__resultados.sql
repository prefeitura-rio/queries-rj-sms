{{
    config(
        alias="resultados",
        materialized="table",
    )
}}

with
    source as (
        select * from {{ source("brutos_biomega_staging", "resultados") }}
    ),
    dedup as (
        select 
            * 
        from source
        qualify row_number() over (partition by id order by datalake_loaded_at desc) = 1
    ),
    renamed as (	
        select
            {{ process_null('id') }} as id_resultado,
            {{ process_null('exame_id') }} as id_exame,

            {{ process_null('codigoLis') }} as codigo_lis,
            {{ process_null('codigoApoio') }} as codigo_apoio,
            {{ process_null('descricaoApoio') }} as descricao_apoio,
            {{ process_null('decimal') }} as decimal,
            {{ process_null('tipo') }} as tipo,
            {{ process_null('resultado') }} as resultado,
            {{ process_null('unidade') }} as unidade,
            {{ process_null('alterado') }} as alterado,
            {{ process_null('valorReferenciaTexto') }} as valor_referencia_texto,
            {{ process_null('valorReferenciaMinimo') }} as valor_referencia_minimo,
            {{ process_null('valorReferenciaMaximo') }} as valor_referencia_maximo,

            safe_cast({{ process_null('datalake_loaded_at') }} as timestamp) as loaded_at
        from dedup
    )
select *
from renamed