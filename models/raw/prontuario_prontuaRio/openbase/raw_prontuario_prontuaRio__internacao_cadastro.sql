{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="internacao_cadastro",
        materialized="table",
        tags=["prontuaRio"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with

source_ as (
  select * from {{ source('brutos_prontuario_prontuaRio_staging', 'intb6') }} 
),

internacao_cadastro as (
    select
        json_extract_scalar(data, '$.ib6regist') as id_registro,
        json_extract_scalar(data, '$.ib6prontuar') as id_prontuario1,
        json_extract_scalar(data, '$.ib6cpfpac') as paciente_cpf,
        concat(
            json_extract_scalar(data, '$.ib6pnome'),
            json_extract_scalar(data, '$.ib6compos')
        ) as paciente_nome,
        json_extract_scalar(data, '$.ib6sexo') as sexo,
        safe.parse_date('%Y%m%d',json_extract_scalar(data, '$.ib6dtnasc')) as paciente_data_nascimento,
        json_extract_scalar(data, '$.ib6pai') as paciente_pai,
        json_extract_scalar(data, '$.ib6mae') as paciente_mae,
        json_extract_scalar(data, '$.ib6natural') as paciente_naturalidade,
        json_extract_scalar(data, '$.ib6nacion') as paciente_nacionalidade,
        json_extract_scalar(data, '$.ib6tipndoc') as paciente_numero_documento,
        json_extract_scalar(data, '$.ib6lograd') as endereco_logradouro,
        json_extract_scalar(data, '$.ib6numero') as endereco_numero,
        json_extract_scalar(data, '$.ib6complem') as endereco_complemento,
        json_extract_scalar(data, '$.ib6bairro') as endereco_bairro,
        json_extract_scalar(data, '$.ib6municip') as endereco_municipio,
        json_extract_scalar(data, '$.ib6uf') as endereco_uf,
        json_extract_scalar(data, '$.ib6cep') as endereco_cep,
        json_extract_scalar(data, '$.ib6telef') as paciente_telefone,
        case 
            when json_extract_scalar(data, '$.ib6dtultaten') in ('00000000','0', '') 
            then cast(null as date)
            when regexp_contains(json_extract_scalar(data, '$.ib6dtultaten'), r'^\d{8}$') 
            then safe.parse_date('%Y%m%d',json_extract_scalar(data, '$.ib6dtultaten')) 
        end as ultimo_atendimento_data,
        case 
            when json_extract_scalar(data, '$.ib6dtcad') in ('00000000','0', '') 
            then cast(null as date)
            when regexp_contains(json_extract_scalar(data, '$.ib6dtcad'), r'^\d{8}$') 
            then safe.parse_date('%Y%m%d',json_extract_scalar(data, '$.ib6dtcad')) 
        end as cadastro_data,
        json_extract_scalar(data, '$.ib6idpron') as id_prontuario2,
        json_extract_scalar(data, '$.ib6depend') as depend, -- Confirmar nome de coluna
        case 
            when json_extract_scalar(data, '$.ib6dtobito') in ('00000000', '0', '') 
            then cast(null as date)
            else safe.parse_date('%Y%m%d', json_extract_scalar(data, '$.ib6dtobito'))
        end as obito_data,
        json_extract_scalar(data, '$.ib6codcs') as codcs, -- Confirmar nome da coluna
        json_extract_scalar(data, '$.ib6cns') as cns,
        json_extract_scalar(data, '$.ib6tipodoc') as documento_tipo,
        json_extract_scalar(data, '$.ib6codmun') as id_municipio,
        json_extract_scalar(data, '$.ib6cod.logra') as id_logradouro,
        cnes,
        loaded_at
    from source_
),

final as (
    select 
        safe_cast(id_registro as int64) as id_registro,
        safe_cast(id_prontuario1 as int64) as id_prontuario1,
        case 
            when paciente_cpf like "%000%"
                then cast(null as string)
            when paciente_cpf = "0"
                then cast(null as string)
            else {{ process_null('paciente_cpf') }}
        end as paciente_cpf,
        paciente_nome,
        {{ process_null('sexo') }} as sexo,
        paciente_data_nascimento,
        {{ process_null('paciente_pai') }} as paciente_pai,
        {{ process_null('paciente_mae') }} as paciente_mae,
        {{ process_null('paciente_naturalidade') }} as paciente_naturalidade,
        {{ process_null('paciente_nacionalidade') }} as paciente_nacionalidade,
        {{ process_null('paciente_numero_documento') }} as paciente_numero_documento,
        {{ process_null('endereco_logradouro') }} as endereco_logradouro,
        {{ process_null('endereco_numero') }} as endereco_numero,
        {{ process_null('endereco_complemento') }} as endereco_complemento,
        {{ process_null('endereco_bairro') }} as endereco_bairro,
        {{ process_null('endereco_municipio') }} as endereco_municipio,
        {{ process_null('endereco_uf') }} as endereco_uf,
        {{ process_null('endereco_cep') }} as endereco_cep,
        {{ process_null('paciente_telefone') }} as paciente_telefone,
        ultimo_atendimento_data,
        cadastro_data,
        {{ process_null('depend') }} as depend,
        {{ process_null('id_prontuario2') }} as id_prontuario2,
        obito_data,
        {{ process_null('codcs') }} as codcs,
        {{ process_null('cns') }} as cns,
        {{ process_null('documento_tipo') }} as documento_tipo,
        {{ process_null('id_municipio') }} as id_municipio,
        {{ process_null('id_logradouro') }} as id_logradouro,
        cnes,
        loaded_at,
        cast(safe_cast(loaded_at as timestamp) as date) as data_particao
    from internacao_cadastro
    qualify row_number() over(partition by id_registro, id_prontuario1, cnes order by loaded_at desc) = 1
)

select 
    concat(cnes, '.', id_prontuario1) as gid_prontuario,
    concat(cnes, '.', id_registro) as gid_registro,
    *
from final