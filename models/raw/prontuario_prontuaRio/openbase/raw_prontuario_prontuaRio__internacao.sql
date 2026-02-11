{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="internacao",
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
    select  
      *
    from {{ source('brutos_prontuario_prontuaRio_staging', 'cen02') }}
  ),

 internacoes as (
  select
      json_extract_scalar(data, '$.i02pront') as id_prontuario,
      json_extract_scalar(data, '$.n02numbolet') as id_boletim,
      json_extract_scalar(data, '$.c02cid10') as codigo_cid10,
      json_extract_scalar(data, '$.c02cpfmedsol') as medico_solicitante_cpf,
      json_extract_scalar(data, '$.c02cpfmedres') as medico_responsavel_cpf,
      json_extract_scalar(data, '$.c02codclin') as id_clinica,
      json_extract_scalar(data, '$.c02codleito') as id_leito,
      json_extract_scalar(data, '$.c02procsol') as id_procsol,
      json_extract_scalar(data, '$.d02inter') as internacao_data,
      json_extract_scalar(data, '$.c02hora') as internacao_hora,
      json_extract_scalar(data, '$.c02carint') as carint,
      json_extract_scalar(data, '$.c02cid') as codigo_cid,
      json_extract_scalar(data, '$.c02estado') as estado,
      json_extract_scalar(data, '$.d02cirurgia') as cirurgia,
      json_extract_scalar(data, '$.c02fora') as fora,
      json_extract_scalar(data, '$.c02clinreal') as clinreal,
      json_extract_scalar(data, '$.c02clinapo1') as clinapo1,
      json_extract_scalar(data, '$.c02clinapo2') as clinapo2,
      json_extract_scalar(data, '$.c02clinapo3') as clinapo3,
      json_extract_scalar(data, '$.c02forment') as forment,
      json_extract_scalar(data, '$.c02codusent') as codusent,
      json_extract_scalar(data, '$.c02origem') as origem,
      json_extract_scalar(data, '$.d02entrada') as entrada,
      json_extract_scalar(data, '$.c02hentrada') as c02hentrada,
      json_extract_scalar(data, '$.c02clinant') as clinant,
      json_extract_scalar(data, '$.c02compos') as compos,
      json_extract_scalar(data, '$.c02evento') as cevento,
      json_extract_scalar(data, '$.c02senha') as senha,
      json_extract_scalar(data, '$.c02acompanha') as acompanha,
      json_extract_scalar(data, '$.c02parentesc') as parentesc,
      json_extract_scalar(data, '$.d02entacomp') as entacomp,
      json_extract_scalar(data, '$.c02hentacomp') as hentacomp,
      json_extract_scalar(data, '$.d02saiacomp') as saiacomp,
      json_extract_scalar(data, '$.c02hsaiacomp') as hsaiacomp,
      json_extract_scalar(data, '$.n02aih') as aih,
      json_extract_scalar(data, '$.c02numcons') as numcons,
      json_extract_scalar(data, '$.c02notifcomp') as notifcomp,
      cnes,
      loaded_at

    from source_
 ),

 final as (
  select 
      safe_cast(id_prontuario as int64) as id_prontuario,
      safe_cast(id_boletim as int64) as id_boletim,
      {{ process_null('codigo_cid10') }} as codigo_cid10,
      case 
        when medico_solicitante_cpf like "%000%"
          then cast(null as string)
        when medico_solicitante_cpf = '0'
          then cast(null as string)
        else {{ process_null('medico_solicitante_cpf') }}
      end as medico_solicitante_cpf,
      case 
        when medico_responsavel_cpf like "%000%"
          then cast(null as string)
        when medico_responsavel_cpf = '0'
          then cast(null as string)
        else {{ process_null('medico_responsavel_cpf') }}
      end as medico_responsavel_cpf,
      {{ process_null('id_clinica') }} as id_clinica,
      {{ process_null('id_leito') }} as id_leito,
      {{ process_null('id_procsol') }} as id_procsol,
      safe.parse_date('%Y%m%d', internacao_data) as internacao_data,
      safe.parse_date('%H%M', internacao_hora) as internacao_hora,
      {{ process_null('carint') }} as carint,
      {{ process_null('codigo_cid') }} as codigo_cid,
      {{ process_null('estado') }} as estado,
      {{ process_null('cirurgia') }} as cirurgia,
      {{ process_null('fora') }} as fora,
      {{ process_null('clinreal') }} as clinreal,
      {{ process_null('clinapo1') }} as clinapo1,
      {{ process_null('clinapo2') }} as clinapo2,
      {{ process_null('clinapo3') }} as clinapo3,
      {{ process_null('forment') }} as forment,
      {{ process_null('codusent') }} as codusent,
      {{ process_null('origem') }} as origem,
      {{ process_null('entrada') }} as entrada,
      {{ process_null('c02hentrada') }} as c02hentrada,
      {{ process_null('clinant') }} as clinant,
      {{ process_null('compos') }} as compos,
      {{ process_null('cevento') }} as cevento,
      {{ process_null('senha') }} as senha,
      {{ process_null('acompanha') }} as acompanha,
      {{ process_null('parentesc') }} as parentesc,
      {{ process_null('entacomp') }} as entacomp,
      {{ process_null('hentacomp') }} as hentacomp,
      {{ process_null('saiacomp') }} as saiacomp,
      {{ process_null('hsaiacomp') }} as hsaiacomp,
      {{ process_null('aih') }} as aih,
      {{ process_null('numcons') }} as numcons,
      {{ process_null('notifcomp') }} as notifcomp,
      cnes,
      loaded_at,
      cast(safe_cast(loaded_at as timestamp) as date) as data_particao
  from internacoes
  qualify row_number() over(
    partition by 
      id_prontuario,
      id_boletim, 
      cnes 
    order by loaded_at desc) = 1
 )

select 
  concat(cnes, '.', id_prontuario) as gid_prontuario,
  concat(cnes, '.', id_boletim) as gid_boletim,
  *
from final
