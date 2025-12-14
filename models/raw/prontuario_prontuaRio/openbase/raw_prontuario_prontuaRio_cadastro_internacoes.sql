{{
    config(
        alias="cadastro_internacoes",
        materialized="table",
        tags=["prontuaRio"],
    )
}}

with 

  source_ as (
    select  
      *
    from {{ source('brutos_prontuario_prontuaRIO', 'cen02') }}
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
      parse_date('%Y%m%d', json_extract_scalar(data, '$.d02inter')) as internacao_data,
      parse_time('%H%M', json_extract_scalar(data, '$.c02hora')) as internacao_hora,
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

 deduplicated as (
  select * from internacoes 
  qualify row_number() over(partition by i02pront, cnes order by loaded_at desc) = 1
 )   

select
    *,
    date(loaded_at) as data_particao 
from deduplicated