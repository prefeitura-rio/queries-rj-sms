{{
    config(
        alias="controle_presenca",
        materialized="table",
        unique_key="id",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_seguir_em_frente_staging", "controle_presenca") }}
    ),
    renamed as (
        select
            {{ adapter.quote("id") }},
            {{ adapter.quote("title") }},
            {{ adapter.quote("current_phase") }} as fase_atual,
            {{ adapter.quote("creator") }} as criado_por,
            {{ adapter.quote("created_at") }} as criado_em,
            {{ adapter.quote("bolsista") }} as id_nome_cpf,
            {{ adapter.quote("qual_periodo_deseja_cadastrar") }}
            as periodo_cadastrado_tipo,
            {{ adapter.quote("semana") }} as periodo_cadastrado_semana,
            {{ adapter.quote("segunda_feira") }} as registro_segunda_feira,
            {{ adapter.quote("terca_feira") }} as registro_terca_feira,
            {{ adapter.quote("quarta_feira") }} as registro_quarta_feira,
            {{ adapter.quote("quinta_feira") }} as registro_quinta_feira,
            {{ adapter.quote("sexta_feira") }} as registro_sexta_feira,
            {{ adapter.quote("data") }} as periodo_cadastrado_dia,
            {{ adapter.quote("registrar") }} as registro_dia,
            {{ adapter.quote("observacoes") }},
            {{ adapter.quote("anexos") }},
            {{ adapter.quote("ano_particao") }},
            {{ adapter.quote("mes_particao") }},
            {{ adapter.quote("data_particao") }}

        from source
    ),

    filtered as (select * from renamed where id_nome_cpf != "")

select
    -- pk
    id,

    -- fk
    id_nome_cpf,
    {{clean_numeric_string('split(id_nome_cpf, '_')[1]')}}

    -- fields
    upper(observacoes) as observacoes,
    anexos,
    periodo_cadastrado_tipo,
    periodo_cadastrado_dia,
    periodo_cadastrado_semana,
    lower({{clean_name_string('registro_dia')}}) as registro_dia,
    lower({{clean_name_string('registro_segunda_feira')}}) as registro_segunda_feira,
    lower({{clean_name_string('registro_terca_feira')}}) as registro_terca_feira,
    lower({{clean_name_string('registro_quarta_feira')}}) as registro_quarta_feira,
    lower({{clean_name_string('registro_quinta_feira')}}) as registro_quinta_feira,
    lower({{clean_name_string('registro_sexta_feira')}}) as registro_sexta_feira,

    -- metadata
    criado_por,
    safe_cast(criado_em as datetime) as criado_em,
    ano_particao,
    mes_particao,
    safe_cast(data_particao as date) as data_particao

from filtered
