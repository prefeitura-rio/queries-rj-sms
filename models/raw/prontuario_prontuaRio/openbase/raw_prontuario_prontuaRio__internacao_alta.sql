{{
    config(
        schema="brutos_prontuario_prontuaRio",
        alias="internacao_alta",
        materialized="table",
        unique_key="id_prontuario",
        tags=["prontuaRio"],
    )
}}

with 

  source_ as (
    select  
      *
    from {{ source('brutos_prontuario_prontuaRio_staging', 'cen15') }}
  ),

 alta_internacao as (
  select
      json_extract_scalar(data, '$.i15pront') as id_prontuario,
      json_extract_scalar(data, '$.n15numbolet') as id_boletim,
      json_extract_scalar(data, '$.d15inter') as internacao_data,
      json_extract_scalar(data, '$.c15hinter') as internacao_hora,
      json_extract_scalar(data, '$.c15carint') as carint,
      json_extract_scalar(data, '$.c15cpfmedsol') as medico_solicitante_cpf,
      json_extract_scalar(data,'$.d15alta') as alta_data,
      json_extract_scalar(data, '$.c15halta') as alta_hora,
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
 
final as (
  select 
      safe_cast(id_prontuario as int64) as id_prontuario,
      safe.parse_date('%Y%m%d',internacao_data) as internacao_data,
      safe.parse_time('%H%M',internacao_hora) as internacao_hora,
      safe.parse_date('%Y%m%d',alta_data) as alta_data,
      safe.parse_time('%H%M',alta_hora) as alta_hora,
      {{ process_null('carint') }} as carint,
      case 
        when medico_solicitante_cpf like '%000%'
          then cast(null as string)
        when medico_solicitante_cpf = '0'
          then cast(null as string)
        else {{ process_null('medico_solicitante_cpf') }}
      end medico_solicitante_cpf,
      {{ process_null('apresentacao') }} as apresentacao,
      {{ process_null('motivo') }} as motivo,
      {{ process_null('estado') }} as estado,
      {{ process_null('id_clinica') }} as id_clinica,
      {{ process_null('id_leito') }} as id_leito,
      {{ process_null('procsol') }} as procsol,
      {{ process_null('procreal') }} as procreal,
      {{ process_null('cid') }} as cid,
      {{ process_null('vivos') }} as vivos,
      {{ process_null('mortos') }} as mortos,
      case
        when medico_alta_cpf like '%000%'
          then cast(null as string)
        when medico_alta_cpf = '0'
          then cast(null as string)
        else {{ process_null('medico_alta_cpf') }}
      end as medico_alta_cpf,
      {{ process_null('forment') }} as forment,
      {{ process_null('codusent') }} as codusent,
      {{ process_null('codussai') }} as codussai,
      {{ process_null('procprin') }} as procprin,
      {{ process_null('procsec') }} as procsec,
      {{ process_null('cidprim') }} as cidprim,
      {{ process_null('cidsec') }} as cidsec,
      {{ process_null('retorno') }} as retorno,
      {{ process_null('origem') }} as origem,
      {{ process_null('anest') }} as anest,
      {{ process_null('acidanest') }} as acidanest,
      {{ process_null('intacid') }} as intacid,
      {{ process_null('infeccao') }} as infeccao,
      {{ process_null('antimicro') }} as antimicro,
      {{ process_null('obitinfec') }} as obitinfec,
      {{ process_null('tipproc') }} as tipproc,
      {{ process_null('supuracao') }} as supuracao,
      {{ process_null('numgest') }} as numgest,
      {{ process_null('gestrisc') }} as gestrisc,
      {{ process_null('prenatal') }} as prenatal,
      {{ process_null('conceptos') }} as conceptos,
      {{ process_null('sexo1') }} as sexo1,
      {{ process_null('sexo2') }} as sexo2,
      {{ process_null('peso1') }} as peso1,
      {{ process_null('peso2') }} as peso2,
      {{ process_null('tipo1') }} as tipo1,
      {{ process_null('tipo2') }} as tipo2,
      {{ process_null('sit1') }} as sit1,
      {{ process_null('sit2') }} as sit2,
      {{ process_null('senha') }} as senha,
      {{ process_null('aih') }} as aih,
      {{ process_null('dtger') }} as dtger,
      {{ process_null('acompanha') }} as acompanha,
      {{ process_null('parentesc') }} as parentesc,
      {{ process_null('entacomp') }} as entacomp,
      {{ process_null('hentacomp') }} as hentacomp,
      {{ process_null('saiacomp') }} as saiacomp,
      {{ process_null('hsaiacomp') }} as hsaiacomp,
      {{ process_null('evento') }} as evento,
      {{ process_null('numcons') }} as numcons,
      {{ process_null('notifcomp') }} as notifcomp,
      {{ process_null('notif2') }} as notif2,
      {{ process_null('codigo_cid10') }} as codigo_cid10,
      {{ process_null('codigo_categoria_cid10') }} as codigo_categoria_cid10,
      {{ process_null('compos3') }} as compos3,
      {{ process_null('codigo_cid10_secundario') }} as codigo_cid10_secundario,
      {{ process_null('intercar') }} as intercar,
      cnes,
      loaded_at,
      cast(safe_cast(loaded_at as timestamp) as date) as data_particao
  from alta_internacao

  qualify row_number() over(
    partition by id_prontuario, cnes
    order by loaded_at desc) = 1
)

 select
  concat(cnes, '.', id_prontuario) as gid_prontuario,
  *
 from final

 