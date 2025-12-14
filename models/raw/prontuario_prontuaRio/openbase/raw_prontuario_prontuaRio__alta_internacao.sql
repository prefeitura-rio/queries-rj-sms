{{
    config(
        alias="alta_internacao",
        materialized="table",
        unique_key="id_prontuario",
        tags=["prontuaRio"],
    )
}}

with 

  source_ as (
    select  
      *
    from {{ source('brutos_prontuario_prontuaRIO', 'cen15') }}
  ),

 alta_internacao as (
  select
      json_extract_scalar(data, '$.i15pront') as id_prontuario,
      json_extract_scalar(data, '$.n15numbolet') as id_boletim,
      parse_date('%Y%m%d',json_extract_scalar(data, '$.d15inter')) as internacao_data,
      parse_time('%H%M%', json_extract_scalar(data, '$.c15hinter')) as internacao_hora,
      json_extract_scalar(data, '$.c15carint') as carint,
      json_extract_scalar(data, '$.c15cpfmedsol') as medico_solicitante_cpf,
      parse_date('%Y%m%d',json_extract_scalar(data,'$.d15alta')) as alta_data,
      parse_time('%H%M',json_extract_scalar(data, '$.c15halta')) as alta_hora,
      json_extract_scalar(data, '$.d15apres') as apresentacao,
      json_extract_scalar(data, '$.c15motivo') as motivo,
      json_extract_scalar(data, '$.c15estado') as estado,
      json_extract_scalar(data, '$.c15codclin') as id_clinica,
      json_extract_scalar(data, '$.c15codleito') as id_leito,
      json_extract_scalar(data, '$.c15procsol') as procsol,
      json_extract_scalar(data, '$.c15procreal') as procreal,
      json_extract_scalar(data, '$.c15cid') as cid,
      json_extract_scalar(data, '$.c15vivos') as vivos, --process null
      json_extract_scalar(data, '$.c15mortos') as mortos, --process null
      json_extract_scalar(data, '$.c15cpfalta') as medico_alta_cpf,
      json_extract_scalar(data, '$.c15forment') as forment,
      json_extract_scalar(data, '$.c15codusent') as codusent,
      json_extract_scalar(data, '$.c15codussai') as codussai,
      json_extract_scalar(data, '$.c15procprin') as procprin,
      json_extract_scalar(data, '$.c15procsec') as procsec,
      json_extract_scalar(data, '$.c15cidprim') as cidprim,
      json_extract_scalar(data, '$.c15cidsec') as cidsec,
      json_extract_scalar(data, '$.d15retorno') as retorno,
      json_extract_scalar(data, '$.c15origem') as origem,
      json_extract_scalar(data, '$.c15anest') as anest, -- process null
      json_extract_scalar(data, '$.c15acidanest') as acidanest,
      json_extract_scalar(data, '$.c15intacid') as intacid,
      json_extract_scalar(data, '$.c15infeccao') as infeccao,
      json_extract_scalar(data, '$.c15antimicro') as antimicro,
      json_extract_scalar(data, '$.c15obitinfec') as obitinfec,
      json_extract_scalar(data, '$.c15tipproc') as tipproc,
      json_extract_scalar(data, '$.c15supuracao') as supuracao,
      json_extract_scalar(data, '$.i15numgest') as numgest,
      json_extract_scalar(data, '$.c15gestrisc') as gestrisc,
      json_extract_scalar(data, '$.c15prenatal') as prenatal,
      json_extract_scalar(data, '$.c15conceptos') as conceptos,
      json_extract_scalar(data, '$.c15sexo1') as sexo1,
      json_extract_scalar(data, '$.c15sexo2') as sexo2,
      json_extract_scalar(data, '$.i15peso1') as peso1,
      json_extract_scalar(data, '$.i15peso2') as peso2,
      json_extract_scalar(data, '$.c15tipo1') as tipo1,
      json_extract_scalar(data, '$.c15tipo2') as tipo2,
      json_extract_scalar(data, '$.c15sit1') as sit1,
      json_extract_scalar(data, '$.c15sit2') as sit2,
      json_extract_scalar(data, '$.c15senha') as senha,
      json_extract_scalar(data, '$.n15aih') as aih,
      json_extract_scalar(data, '$.d15dtger') as dtger,
      json_extract_scalar(data, '$.c15acompanha') as acompanha,
      json_extract_scalar(data, '$.c15parentesc') as parentesc,
      json_extract_scalar(data, '$.d15entacomp') as entacomp,
      json_extract_scalar(data, '$.c15hentacomp') as hentacomp,
      json_extract_scalar(data, '$.d15saiacomp') as saiacomp,
      json_extract_scalar(data, '$.c15hsaiacomp') as hsaiacomp,
      json_extract_scalar(data, '$.c15evento') as evento,
      json_extract_scalar(data, '$.c15numcons') as numcons,
      json_extract_scalar(data, '$.c15notifcomp') as notifcomp,
      json_extract_scalar(data, '$.c15notif2') as notif2,
      json_extract_scalar(data, '$.c15cid10') as codigo_cid10,
      json_extract_scalar(data, '$.c15categ10') as codigo_categoria_cid10,
      json_extract_scalar(data, '$.c15compos3') as compos3,
      json_extract_scalar(data, '$.c15cidsec10') as codigo_cid10_secundario,
      json_extract_scalar(data, '$.c15intercar') as intercar,
      cnes,
      loaded_at
  from source_
 ),
 
 deduplicated as (
  select * from alta_internacao 
  qualify row_number() over(partition by id_prontuario, cnes order by loaded_at desc) = 1
 )

 select *, date(loaded_at) as data_particao
 from deduplicated

 