{{
    config(
        schema = 'intermediario_exames_laboratoriais',
        alias="cientificalab",
        materialized="table",
        tags=["exames_laboratoriais"],
        meta = {"owner": "daniel", "team": "cit"}
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with 
  solicitacoes as (
    select
      id,
      laudo_url,
      unidade,
      datahora_pedido,
      paciente_nome,
      paciente_data_nascimento,
      paciente_sexo,
      paciente_cpf,
      paciente_cns,
      paciente_nome_mae,
      loaded_at
    from {{ ref('raw_exames_laboratoriais__solicitacoes') }}
    where origem = 'cientificalab'
  ),

  exames as (
    select 
      id,
      id_solicitacao,
      codigo_apoio,
      descricao_metodo,
      data_assinatura,
      solicitante_numero,
      solicitante_conselho,
      solicitante_nome,
      mensagem,
      datalake_loaded_at
    from {{ ref('raw_exames_laboratoriais__exames') }}
  ),

  resultados as (
      select 
        res.id, 
        res.id_exame,    
        ex.id_solicitacao, 
        res.codigo_apoio,
        res.descricao_apoio,
        res.resultado,
        res.unidade,
        res.alterado
      from {{ ref('raw_exames_laboratoriais__resultados') }} res
      inner join {{ ref('raw_exames_laboratoriais__exames') }} ex 
        on res.id_exame = ex.id
    ),
  
    resultados_agrupados as (
      select 
        {{ dbt_utils.generate_surrogate_key(['id_solicitacao', 'id_exame']) }} as id_exame, 
        array_agg(
          struct(
            codigo_apoio,
            descricao_apoio,
            resultado as valor,
            unidade,
            alterado
          )
        ) as resultados
      from resultados
      group by {{ dbt_utils.generate_surrogate_key(['id_solicitacao', 'id_exame']) }}
    ),

  estabelecimento as (
    select 
      nome_original,
      nome_padronizado,
      cnes,
      ap
    from {{ ref('raw_sheets__unidades_exames_laboratoriais') }} 
  ),

  exames_laboratoriais as (
    select 
      es.cnes as id_cnes,
      so.id as id_solicitacao_fonte,
      ex.id as id_exame_fonte,
      {{ dbt_utils.generate_surrogate_key(['so.id']) }} as id_solicitacao,
      {{ dbt_utils.generate_surrogate_key(['so.id', 'ex.id']) }} as id_exame,
      es.nome_padronizado as unidade_nome,
      es.ap as unidade_ap,
      {{ proper_br('so.paciente_nome') }} as paciente_nome,
      cast(so.paciente_data_nascimento as date) as paciente_nascimento_data,
      so.paciente_cns,
      so.paciente_cpf,
      case 
        when so.paciente_sexo = 'male' then 'masculino'
        when so.paciente_sexo = 'female' then 'feminino'
        else null
      end as paciente_sexo,
      cast(so.datahora_pedido as datetime) as exame_coleta_datahora,
      cast(so.datahora_pedido as date) as exame_data,
      cast(null as string) as exame_nome,
      ex.codigo_apoio as exame_codigo,
      cast(null as string) as exame_tipo,
      cast(null as string) as exame_metodo,
      cast(null as string) as exame_laudo_descricao,
      so.laudo_url as exame_laudo,
      case 
        when ex.mensagem = 'LIBERADO' then 'concluído'
        when trim(ex.mensagem) in ('', null) then null
        else lower(ex.mensagem)
      end as exame_status,
      cast(ex.data_assinatura as datetime) as exame_laudo_datahora,
      re.resultados,

      cast(null as string) as profissional_solicitante_cbo,
      cast(null as string) as profissional_solicitante_cpf,
      ex.solicitante_numero as profissional_solicitante_crm,
      {{ proper_br('ex.solicitante_nome') }} as profissional_solicitante_nome,

      cast(null as string) as profesional_laudista_cbo,
      cast(null as string) as profissional_laudista_cpf,
      cast(null as string) as profissional_laudista_nome,
      cast(ex.datalake_loaded_at as datetime) as loaded_at,
      cast(ex.datalake_loaded_at as date) as data_particao
      
    from exames ex
    inner join solicitacoes so 
      on ex.id_solicitacao = so.id
    left join estabelecimento es 
      on so.unidade = es.nome_original
    left join resultados_agrupados re 
      on {{ dbt_utils.generate_surrogate_key(['so.id', 'ex.id']) }} = re.id_exame
  )

select * from exames_laboratoriais