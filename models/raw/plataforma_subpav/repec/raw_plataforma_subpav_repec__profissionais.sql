{{
    config(
        alias="repec__profissionais",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__profissionais") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_profissional') }} as id_profissional,
            {{ process_null('cnes_entidade') }} as cnes_entidade,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('nome_prof') ~ " as string)") }}) as nome_prof,
            {{ process_null('data_nasc_prof') }} as data_nasc_prof,
            {{ process_null('sexo_prof') }} as sexo_prof,
            {{ process_null('cpf_prof') }} as cpf_prof,
            {{ process_null('cns_prof') }} as cns_prof,
            {{ process_null('cbo') }} as cbo,
            {{ process_null('cedula_prof') }} as cedula_prof,
            {{ process_null('email') }} as email,
            {{ process_null('residente') }} as residente,
            {{ process_null('estagiario') }} as estagiario,
            {{ process_null('id_perceptor') }} as id_perceptor,
            {{ process_null('ativo') }} as ativo,
            {{ process_null('data_inicio_serv') }} as data_inicio_serv,
            {{ process_null('data_fim_serv') }} as data_fim_serv,
            {{ process_null('origem_arquivo') }} as origem_arquivo,
            {{ process_null('origem_banco') }} as origem_banco,
            {{ repec_origem_unidade_para_cnes("origem_unidade") }} as cnes_origem,
            safe_cast({{ process_null('datalake_loaded_at') }} as timestamp) as datalake_loaded_at
        from source
    ),

    deduplicar as (
        select *
        from tratar_campos
        qualify row_number() over (
            partition by
                    id_profissional,
                    cnes_entidade,
                    nome_prof,
                    data_nasc_prof,
                    sexo_prof,
                    cpf_prof,
                    cns_prof,
                    cbo,
                    cedula_prof,
                    email,
                    residente,
                    estagiario,
                    id_perceptor,
                    ativo,
                    data_inicio_serv,
                    data_fim_serv
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
