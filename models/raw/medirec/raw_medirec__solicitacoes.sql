{{
    config(
        alias="solicitacao",
        materialized="table",
    )
}}

with 
    source as (
        select * from {{ source('brutos_medirec_staging', 'cielab_sectionitems_sms') }}
    ),

    solicitacoes as (
        select
            {{ process_null("exam_id") }} as solicitacao_id,
            {{ process_null("section_id") }} as section_id,
            {{ process_null("section_exame") }} as section_exame,
            {{ process_null("section_metodo") }} as section_metodo,
            {{ process_null("section_status") }} as section_status,
            {{ process_null("section_diluicao") }} as section_diluicao,
            {{ process_null("section_omite") }} as section_omite,
            {{ process_null("section_calcCod") }} as section_calcCod,
            {{ process_null("section_troxeMat") }} as section_troxeMat,
            {{ process_null("section_temObs") }} as section_temObs,
            {{ process_null("item_id") }} as item_id,
            {{ process_null("item_parametros") }} as item_parametros,
            {{ process_null("item_resultado") }} as item_resultado,
            {{ process_null("item_calculado") }} as item_calculado,
            {{ process_null("item_unidMed") }} as item_unidMed,
            {{ process_null("item_flag") }} as item_flag
        from source
    )

select * from solicitacoes