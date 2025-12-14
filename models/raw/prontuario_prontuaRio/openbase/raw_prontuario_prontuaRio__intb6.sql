{{
    config(
        alias="interb6",
        materialized="table",
        tags=["prontuaRio"],
    )
}}

with

source_ as (
  select * from {{ source('brutos_prontuario_prontuaRIO', 'intb6') }} 
),

intb6 as (
    select
        json_extract_scalar(data, '$.ib6regist') as id_registo,
        json_extract_scalar(data, '$.ib6prontuar') as id_prontuario,
        concat(
            json_extract_scalar(data, '$.ib6pnome'),
            json_extract_scalar(data, '$.ib6compos')
        ) as paciente_nome,
        json_extract_scalar(data, '$.ib6sexo') as sexo,
        parse_date('%Y%m%d',json_extract_scalar(data, '$.ib6dtnasc')) as paciente_data_nascimento,
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
            then parse_date('%Y%m%d',json_extract_scalar(data, '$.ib6dtultaten')) 
        end as ultimo_atendimento_data,
        case 
            when json_extract_scalar(data, '$.ib6dtcad') in ('00000000','0', '') 
            then cast(null as date)
            when regexp_contains(json_extract_scalar(data, '$.ib6dtcad'), r'^\d{8}$') 
            then parse_date('%Y%m%d',json_extract_scalar(data, '$.ib6dtcad')) 
        end as cadastro_data,
        json_extract_scalar(data, '$.ib6idpron') as id_prontuario,
        json_extract_scalar(data, '$.ib6depend') as depend, -- Confirmar nome de coluna
        case 
            when json_extract_scalar(data, '$.ib6dtobito') in ('00000000', '0', '') 
            then cast(null as date)
            else parse_date('%Y%m%d', json_extract_scalar(data, '$.ib6dtobito'))
        end as obito_data,
        json_extract_scalar(data, '$.ib6codcs') as codcs, -- Confirmar nome da coluna
        json_extract_scalar(data, '$.ib6cns') as cns,
        json_extract_scalar(data, '$.ib6tipodoc') as documento_tipo,
        json_extract_scalar(data, '$.ib6codmun') as id_municipio,
        json_extract_scalar(data, '$.ib6cpfpac') as paciente_cpf,
        json_extract_scalar(data, '$.ib6cod.logra') as id_logradouro,
        cnes,
        loaded_at
    from source_
),

deduplicated as (
  select * from intb6 
  qualify row_number() over(partition by id_registro, id_prontuario, cnes order by loaded_at desc) = 1
)

select *, date(loaded_at) as data_particao from deduplicated