{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="cen62",
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
  select * 
  from {{ source('brutos_prontuario_prontuaRio_staging', 'cen62') }}
  ),

  cadastro as (
    select
        json_extract_scalar(data, '$.n62numbolet') as id_boletim,
        json_extract_scalar(data, '$.d62inter') as internacao_data,
        case 
            when json_extract_scalar(data, '$.d62inter') in ('', '00000000') 
                then null
            when regexp_contains(json_extract_scalar(data, '$.c62hinter'), r'^\d{4}$') 
                then safe.parse_time('%H%M',json_extract_scalar(data, '$.c62hinter'))
            else null
        end as internacao_hora,
        json_extract_scalar(data, '$.d62alta') as alta_data,
        json_extract_scalar(data, '$.c62halta') as alta_hora,
        json_extract_scalar(data, '$.c62sexo') as paciente_sexo,
        safe_cast(json_extract_scalar(data, '$.i62idade') as int64) as paciente_idade,
        json_extract_scalar(data, '$.i62tipidade') as tipidade,
        json_extract_scalar(data, '$.c62setor') as setor,
        json_extract_scalar(data, '$.c62cid') as cid,
        json_extract_scalar(data, '$.c62cid1') as cid1,
        json_extract_scalar(data, '$.c62cid2') as cid2,
        json_extract_scalar(data, '$.c62origem') as origem,
        json_extract_scalar(data, '$.c62motivo') as motivo,
        json_extract_scalar(data, '$.c62tipalta') as tipo_alta,
        json_extract_scalar(data, '$.c62cpfalta') as cpf_alta,
        json_extract_scalar(data, '$.c62codussai') as codussai,
        json_extract_scalar(data, '$.c62motatend') as motivo_atendimento, -- Confirmar nome da coluna
        json_extract_scalar(data, '$.c62casopol') as caso_policial,
        json_extract_scalar(data, '$.c62trauma') as trauma,
        json_extract_scalar(data, '$.c62plsaude') as plano_saude,
        json_extract_scalar(data, '$.c62acidtrab') as acidente_trabalho,
        json_extract_scalar(data, '$.c62ambulan') as ambulancia,
        json_extract_scalar(data, '$.c62tempobt') as tempobt,
        json_extract_scalar(data, '$.c62notif') as notif,
        json_extract_scalar(data, '$.c62notif1') as notif1,
        json_extract_scalar(data, '$.c62notif2') as notif2,
        json_extract_scalar(data, '$.n62diasatest') as diasatest,
        json_extract_scalar(data, '$.c62cid10') as cid10_1,
        json_extract_scalar(data, '$.c62categ10') as categoria_cid10_1,
        json_extract_scalar(data, '$.c62compos2') as compos2,
        json_extract_scalar(data, '$.c62cid210') as cid10_2,
        cnes,
        loaded_at
    from source_
  ),

final as (
    select
        safe_cast(id_boletim as int64) as id_boletim,
        internacao_data,
        internacao_hora,
        alta_data,
        alta_hora,
        {{ process_null('paciente_sexo') }} as paciente_sexo,
        paciente_idade,
        {{ process_null('tipidade') }} as tipidade,
        {{ process_null('setor') }} as setor,
        {{ process_null('cid') }} as cid,
        {{ process_null('cid1') }} as cid1,
        {{ process_null('cid2') }} as cid2,
        {{ process_null('origem') }} as origem,
        {{ process_null('motivo') }} as motivo,
        {{ process_null('tipo_alta') }} as tipo_alta,
        case 
            when cpf_alta like "%000%" 
                then cast(null as string)
            else {{ process_null('cpf_alta') }}
        end as cpf_alta,
        {{ process_null('codussai') }} as codussai,
        {{ process_null('motivo_atendimento') }} as motivo_atendimento,
        {{ process_null('caso_policial') }} as caso_policial,
        {{ process_null('trauma') }} as trauma,
        {{ process_null('plano_saude') }} as plano_saude,
        {{ process_null('acidente_trabalho') }} as acidente_trabalho,
        {{ process_null('ambulancia') }} as ambulancia,
        {{ process_null('tempobt') }} as tempobt,
        {{ process_null('notif') }} as notif,
        {{ process_null('notif1') }} as notif1,
        {{ process_null('notif2') }} as notif2,
        {{ process_null('diasatest') }} as diasatest,
        {{ process_null('cid10_1') }} as cid10_1,
        {{ process_null('categoria_cid10_1') }} as categoria_cid10_1,
        {{ process_null('compos2') }} as compos2,
        {{ process_null('cid10_2') }} as cid10_2,
        cnes,
        loaded_at,
        cast(safe_cast(loaded_at as timestamp) as date) as data_particao,
    from cadastro
    qualify row_number() over(partition by id_boletim, cnes order by loaded_at desc) = 1
)

select 
    concat(cnes, '.', id_boletim) as gid_boletim,
    *
from final

