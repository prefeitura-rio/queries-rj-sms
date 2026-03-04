{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="emergencia_resumo",
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
    from {{ source('brutos_prontuario_prontuaRio_staging', 'cen54') }}
  ),

 cadastro_emergencia as (
  select
      json_extract_scalar(data, '$.n54numbolet') as id_boletim,
      json_extract_scalar(data, '$.i54pront') as id_prontuario,
      concat(
        json_extract_scalar(data, '$.c54pident'),
        json_extract_scalar(data, '$.c54compos1')
      ) as paciente_nome,
      case trim(upper(json_extract_scalar(data, '$.c54sexo')))
        when '1' then 'Masculino'
        when '3' then 'Feminino'
        else 'Outro'
      end as paciente_sexo,
      safe.parse_date('%Y%m%d',json_extract_scalar(data, '$.d54nasc')) as paciente_data_nascimento, -- TODO: Tratar data
      safe_cast(json_extract_scalar(data, '$.i54idade') as int64) as paciente_idade,
      json_extract_scalar(data, '$.i54tipidade') as tipo_idade, -- TODO: Confirmar nome da coluna
      json_extract_scalar(data, '$.c54natural') as naturalidade,
      json_extract_scalar(data, '$.c54nacion') as nacionalidade,
      json_extract_scalar(data, '$.c54cpfpac') as paciente_cpf,
      json_extract_scalar(data, '$.c54pai') as paciente_pai_nome,
      json_extract_scalar(data, '$.c54mae') as paciente_mae_nome,
      json_extract_scalar(data, '$.c54resp') as paciente_responsavel,
      json_extract_scalar(data, '$.c54end') as paciente_endereco,
      json_extract_scalar(data, '$.c54numero') as paciente_endereco_numero,
      json_extract_scalar(data, '$.c54complem') as paciente_endereco_complemento,
      json_extract_scalar(data, '$.c54bairro') as paciente_bairro,
      json_extract_scalar(data, '$.c54mun') as paciente_municipio,
      json_extract_scalar(data, '$.c54uf') as paciente_uf,
      json_extract_scalar(data, '$.c54cep') as paciente_cep,
      json_extract_scalar(data, '$.c54tel') as paciente_telefone,
      json_extract_scalar(data, '$.c54setor') as setor,

      case
        when json_extract_scalar(data, '$.d54inter') in ('00000000','0', '', '99999999') 
          then cast(null as date)
        when regexp_contains(json_extract_scalar(data, '$.d54inter'), r'^\d{8}$') 
          then coalesce(
            safe.parse_date('%Y%m%d', json_extract_scalar(data, '$.d54inter')),
            safe.parse_date('%Y%d%m', json_extract_scalar(data, '$.d54inter'))
          )
        else null
      end as internacao_data,

      case
        when json_extract_scalar(data, '$.c54hinter') in ('0', '', '9999') 
          then cast(null as time)
        when regexp_contains(json_extract_scalar(data, '$.c54hinter'), r'^\d{4}$') 
          then safe.parse_time('%H%M', json_extract_scalar(data, '$.c54hinter'))
        else null
      end as internacao_hora,
      json_extract_scalar(data, '$.c54cid') as cid, -- process null
      json_extract_scalar(data, '$.c54estado') as estado,
      json_extract_scalar(data, '$.c54origem') as origem,
      case 
          when json_extract_scalar(data, '$.d54alta') in ('00000000','0', '', '99999999') 
            then cast(null as date)
          when regexp_contains(json_extract_scalar(data, '$.d54alta'), r'^\d{8}$') 
            then coalesce(
              safe.parse_date('%Y%m%d', json_extract_scalar(data, '$.d54alta')),
              safe.parse_date('%Y%d%m', json_extract_scalar(data, '$.d54alta'))
            )
          else null
      end as alta_data,
      case 
          when json_extract_scalar(data, '$.c54halta') in ('0', '', '9999') 
            then cast(null as time)
          when regexp_contains(json_extract_scalar(data, '$.c54halta'), r'^\d{4}$') 
            then safe.parse_time('%H%M', json_extract_scalar(data, '$.c54halta'))
          else null
      end as alta_hora,
      json_extract_scalar(data, '$.c54motivo') as motivo,
      json_extract_scalar(data, '$.c54tipalta') as tipo_alta,
      json_extract_scalar(data, '$.c54cpfalta') as medico_responsavel_alta_cpf,
      json_extract_scalar(data, '$.c54codussai') as codussai,
      json_extract_scalar(data, '$.c54motatend') as motivo_atendimento,-- Confirmar nome da coluna
      json_extract_scalar(data, '$.c54casopol') as caso_policial, -- Confirmar nome da coluna
      json_extract_scalar(data, '$.c54trauma') as trauma,
      json_extract_scalar(data, '$.c54plsaude') as plano_saude,-- Confrimar nome da coluna
      json_extract_scalar(data, '$.c54acidtrab') as acidente_trabalho,
      json_extract_scalar(data, '$.c54ambulan') as ambulancia, 
      json_extract_scalar(data, '$.c54cid1') as codigo_cid1,
      json_extract_scalar(data, '$.c54cid2') as codigo_cid2,
      json_extract_scalar(data, '$.c54tempobt') as tempobt,
      json_extract_scalar(data, '$.c54notif') as notif,
      json_extract_scalar(data, '$.c54notif1') as notif1,
      json_extract_scalar(data, '$.c54notif2') as notif2,
      json_extract_scalar(data, '$.n54diasatest') as diasatest,
      json_extract_scalar(data, '$.c54cid10') as codigo_cid10_1,
      json_extract_scalar(data, '$.c54categ110') as categoria_cid10_1,
      json_extract_scalar(data, '$.c54compos3') as compos3,
      json_extract_scalar(data, '$.c54cid210') as codigo_cid10_2,
      json_extract_scalar(data, '$.c54cns') as cns,
      json_extract_scalar(data, '$.c54tipndoc') as tipndoc,
      json_extract_scalar(data, '$.c54tipodoc') as tipodoc,
      cnes, 
      loaded_at
  from source_
 ),

  final as (
    select
        safe_cast(id_boletim as int64) as id_boletim,
        safe_cast(id_prontuario as int64) as id_prontuario,
        {{ process_null('paciente_nome') }} as paciente_nome,
        {{ process_null('paciente_sexo') }} as paciente_sexo,
        paciente_data_nascimento,
        paciente_idade,
        {{ process_null('tipo_idade') }} as tipo_idade,
        {{ process_null('naturalidade') }} as naturalidade,
        {{ process_null('nacionalidade') }} as nacionalidade,
        case
          when paciente_cpf like '%000%' or paciente_cpf = '0'
            then cast(null as string)
          else {{ process_null('paciente_cpf') }}
        end as paciente_cpf,
        {{ process_null('paciente_pai_nome') }} as paciente_pai_nome,
        {{ process_null('paciente_mae_nome') }} as paciente_mae_nome,
        {{ process_null('paciente_responsavel') }} as paciente_responsavel,
        {{ process_null('paciente_endereco') }} as paciente_endereco,
        {{ process_null('paciente_endereco_numero') }} as paciente_endereco_numero,
        {{ process_null('paciente_endereco_complemento') }} as paciente_endereco_complemento,
        {{ process_null('paciente_bairro') }} as paciente_bairro,
        {{ process_null('paciente_municipio') }} as paciente_municipio,
        {{ process_null('paciente_uf') }} as paciente_uf,
        {{ process_null('paciente_cep') }} as paciente_cep,
        {{ process_null('paciente_telefone') }} as paciente_telefone,
        {{ process_null('setor') }} as setor,
        internacao_data,
        internacao_hora,
        {{ process_null('cid') }} as cid,
        {{ process_null('estado') }} as estado,
        {{ process_null('origem') }} as origem,
        alta_data,
        alta_hora,
        {{ process_null('motivo') }} as motivo,
        {{ process_null('tipo_alta') }} as tipo_alta,
        case 
          when medico_responsavel_alta_cpf like '%000%'
            then cast(null as string)
          when medico_responsavel_alta_cpf = '0'
            then cast(null as string)
          else {{ process_null('medico_responsavel_alta_cpf') }}
        end as medico_responsavel_alta_cpf,
        {{ process_null('codussai') }} as codussai,
        {{ process_null('motivo_atendimento') }} as motivo_atendimento,
        {{ process_null('caso_policial') }} as caso_policial,
        {{ process_null('trauma') }} as trauma,
        {{ process_null('plano_saude') }} as plano_saude,
        {{ process_null('acidente_trabalho') }} as acidente_trabalho,
        {{ process_null('ambulancia') }} as ambulancia,
        {{ process_null('codigo_cid1') }} as codigo_cid1,
        {{ process_null('codigo_cid2') }} as codigo_cid2,
        {{ process_null('tempobt') }} as tempobt,
        {{ process_null('notif') }} as notif,
        {{ process_null('notif1') }} as notif1,
        {{ process_null('notif2') }} as notif2,
        {{ process_null('diasatest') }} as diasatest,
        {{ process_null('codigo_cid10_1') }} as codigo_cid10_1,
        {{ process_null('categoria_cid10_1') }} as categoria_cid10_1,
        {{ process_null('compos3') }} as compos3,
        {{ process_null('codigo_cid10_2') }} as codigo_cid10_2,
        {{ process_null('cns') }} as cns,
        {{ process_null('tipndoc') }} as tipndoc,
        {{ process_null('tipodoc') }} as tipodoc,
        cnes,
        loaded_at,
        cast(safe_cast(loaded_at as timestamp) as date) as data_particao
    from cadastro_emergencia
    qualify row_number() over(partition by id_boletim, id_prontuario, cnes order by loaded_at desc) = 1
  )

select 
  concat(cnes,'.',id_boletim) as gid_boletim,
  concat(cnes,'.',id_prontuario) as gid_prontuario,
  *
from final