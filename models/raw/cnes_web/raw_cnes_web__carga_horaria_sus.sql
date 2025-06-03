{{
    config(
        alias="carga_horaria_sus",
    )
}}


with 

source as (
    select 
        * 
    from {{ source("brutos_cnes_web_staging", "tbCargaHorariaSus") }}
),

renamed as (
    select
        {{ process_null('co_unidade') }} as id_unidade,
        {{ process_null('co_profissional_sus') }} as id_profissional_sus,
        {{ process_null('co_cbo') }} as id_cbo,
        if (tp_sus_nao_sus = 'S', true, false) as atende_sus_indicador,
        {{ process_null('ind_vinculacao') }} as vinculacao,
        {{ process_null('qt_carga_horaria_ambulatorial') }} as carga_horaria_ambulatorial,
        {{ process_null('qt_carga_hor_hosp_sus') }} as carga_horaria_hospitalar,
        {{ process_null('qt_carga_horaria_outros') }} as carga_horaria_outros,
        {{ process_null('co_conselho_classe') }} as conselho_tipo,
        {{ process_null('nu_registro') }} as id_registro_conselho,
        {{ process_null('sg_uf_crm') }} as sigla_uf_crm,
        if (tp_preceptor = '1', true, false) as preceptor_indicador,
        if (tp_residente = '1', true, false) as residente_indicador,
        {{ process_null('nu_cnpj_detalhamento_vinculo') }} as cnpj_empregador,
        safe_cast(dt_atualizacao_origem as date format 'DD/MM/YYYY') as data_atualizacao_origem,
        safe_cast(dt_atualizacao as date format 'DD/MM/YYYY') as data_atualizacao,
        {{ process_null('ano_particao') }} as ano_particao,
        {{ process_null('mes_particao') }} as mes_particao,
        concat(ano_particao, '-', mes_particao, '-', '01') as data_particao,
        cast(_data_carga as datetime) as data_carga,
        {{ process_null('_data_snapshot')}} as data_snapshot

    from source
    qualify row_number() over (
        partition by 
            id_unidade, 
            id_profissional_sus, 
            vinculacao
        order by data_particao desc
    ) = 1
)

select * from renamed
