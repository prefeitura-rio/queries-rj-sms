{{
    config(
        alias="cen62",
        materialized="table",
        tags=["prontuaRio"],
    )
}}


with
  source_ as (
  select * 
  from {{ source('brutos_prontuario_prontuaRIO', 'cen62') }}
  ),

  cadastro as (
    select
        json_extract_scalar(data, '$.n62numbolet') as id_boletim,
        case json_extract_scalar(data, '$.d62inter') 
            when '00000000' then null
            else parse_date('%Y%m%d',json_extract_scalar(data, '$.d62inter'))
        end as interncacao_data,
        case json_extract_scalar(data, '$.c62hinter') 
            when '' then null 
            else parse_time('%H%M', json_extract_scalar(data, '$.c62hinter'))
        end as internacao_hora,
        case json_extract_scalar(data, '$.d62alta')
            when '00000000' then null
            else parse_date('%Y%m%d',json_extract_scalar(data, '$.d62alta'))
        end as alta_data,
        case json_extract_scalar(data, '$.c62halta') 
            when '' then null
            else parse_time('%H%M', json_extract_scalar(data, '$.c62halta'))
        end as alta_hora,
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

  deduplicated as (
    select * from cadastro
    qualify row_number() over(partition by id_boletim, cnes order by loaded_at desc) = 1
  )

select *, date(loaded_at) as data_particao
from deduplicated

