{{
    config(
        alias="preparo_final",
        tags="smsrio"
    )
}}
with 
    source as (
        select * from {{ source('brutos_plataforma_smsrio_staging','transparencia__tb_preparos_finais') }}
    ),
    most_recent as (
        select * from source
        qualify row_number() over (partition by id order by data_extracao desc) = 1
    ),
    transform as (
        select 

            -- Chave Primária
            {{process_null('id')}} as id,
            {{process_null('cnesunidade')}} as id_cnes,
            {{process_null('codigoprocedimentointerno')}} as codigo_procedimento_interno,
            {{process_null('nomeprocedimento')}} as nome_procedimento,
            {{process_null('preparoprocedimento')}} as preparo_procedimento,
            {{process_null('preparoprocedimentotagshtml')}} as preparo_procedimento_tags_html,
            CASE
                WHEN safe_cast({{ process_null('ativo') }} as int64) = 1 THEN TRUE
                WHEN safe_cast({{ process_null('ativo') }} as int64) = 0 THEN FALSE
                ELSE NULL
            END AS ativo,
            safe_cast({{process_null('data_extracao')}} as date) as data_extracao,
            datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at
           
        from most_recent
    )
select * from transform