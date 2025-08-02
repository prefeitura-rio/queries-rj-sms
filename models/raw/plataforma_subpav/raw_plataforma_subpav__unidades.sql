{{
    config(
        alias="unidades",
    )
}}

with
    source as (
        select *
        from {{ source('brutos_plataforma_subpav_staging','views_ipp__Unidades_OSA') }}
    ),
    most_recent as (
        select * 
        from source
        qualify row_number() over (partition by cnes, cod_ine order by datalake_loaded_at desc) = 1
    ),

    -- ------------------------------------------------------------
    -- Limpeza de dados
    -- ------------------------------------------------------------	
    dados_limpos as (
        select
            {{ process_null('cap') }} as cap,
            {{ process_null('ap') }} as ap,
            {{ process_null('cnes') }} as id_cnes,
            {{ process_null('tipo_unidade_aps') }} as unidade_tipo_aps,
            {{ process_null('nome_fantasia') }} as unidade_nome_fantasia,
            # Equipe
            {{ process_null('cod_equipe') }} as equipe_codigo,
            {{ process_null('cod_ine') }} as equipe_id_ine,
            {{ process_null('tipo_eqp') }} as equipe_tipo,
            {{ process_null('nome_area') }} as equipe_nome,
            {{ process_null('cod_area') }} as equipe_id_area,
            {{ process_null('medicos') }} as equipe_medicos,
            {{ process_null('enfermeiros') }} as equipe_enfermeiros,
            {{ process_null('telefone_eqp') }} as equipe_telefone,

            # Endereço
            {{ process_null('end_') }} as endereco,
            {{ process_null('logradouro') }} as logradouro,
            {{ process_null('bairro') }} as bairro,
            {{ process_null('numero') }} as numero,
            {{ process_null('complemento') }} as complemento,
            {{ process_null('cod_cep') }} as cod_cep,

            # Contato
            {{ process_null('telefone') }} as telefone,
            {{ process_null('celular') }} as celular,
            {{ process_null('e_mail') }} as email_principal,
            {{ process_null('e_mail2') }} as email_secundario,
            {{ process_null('site') }} as website,
            {{ process_null('facebook') }} as facebook,
            {{ process_null('instagram') }} as instagram,
            {{ process_null('twitter') }} as twitter,

            # Horário de funcionamento
            NULLIF({{ process_null('hora1') }}, '0') as funcionamento_dia_util_inicio,
            NULLIF({{ process_null('hora2') }}, '0') as funcionamento_dia_util_fim,
            NULLIF({{ process_null('horasab1') }}, '0') as funcionamento_sabado_inicio,
            NULLIF({{ process_null('horasab2') }}, '0') as funcionamento_sabado_fim,

            CASE 
                WHEN {{ process_null('f24horas') }} = '1' THEN 'sim' 
                ELSE 'nao' 
            END as funciona_24_horas,

            safe_cast({{ process_null('dt_ativa') }} as date) as dt_ativa,
            safe_cast({{ process_null('datalake_loaded_at') }} as timestamp) as datalake_loaded_at
        from most_recent
    )

select *
from dados_limpos
order by id_cnes, equipe_id_ine
