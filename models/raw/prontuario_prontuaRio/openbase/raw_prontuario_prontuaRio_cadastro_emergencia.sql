{{
    config(
        alias="cadastro_emergencia",
        materialized="table",
        tags=["prontuaRio"],
    )
}}

with 
  source_ as (
    select  
      *
    from {{ source('brutos_prontuario_prontuaRIO', 'cen54') }}
  ),

 cadastro_emergencia as (
  select
      json_extract_scalar(data, '$.n54numbolet') as id_boletim,
      json_extract_scalar(data, '$.i54pront') as id_prontuario,
      concat(
        json_extract_scalar(data, '$.c54pident'),
        json_extract_scalar(data, '$.c54compos1')
      ) as paciente_nome,
      json_extract_scalar(data, '$.c54sexo') as paciente_sexo,
      json_extract_scalar(data, '$.d54nasc') as paciente_data_nascimento, -- TODO: Tratar data
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
      json_extract_scalar(data, '$.d54inter') as internacao_data, -- TODO: Tratar data
      json_extract_scalar(data, '$.c54hinter') as internacao_hora, -- TODO: Tratar hora
      json_extract_scalar(data, '$.c54cid') as cid, -- process null
      json_extract_scalar(data, '$.c54estado') as estado,
      json_extract_scalar(data, '$.c54origem') as origem,
      json_extract_scalar(data, '$.d54alta') as alta_data, -- TODO: Tratar data
      json_extract_scalar(data, '$.c54halta') as alta_hora, -- TODO: Tratar hora
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

 deduplicated as (
  select * from cadastro_emergencia 
  qualify row_number() over(partition by id_prontuario, id_boletim, cnes order by loaded_at desc) = 1
 )

 select *, date(loaded_at) as data_particao
 from deduplicated