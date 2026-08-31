{{
    config(
        schema = 'intermediario_exames_laboratoriais',
        alias="blessing",
        materialized="table",
        tags=["exames_laboratoriais"],
        meta = {"owner": "daniel", "team": "cit"},
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
  blessing as (
    select *
    from {{ ref('raw_rmd__exames_laboratoriais') }}
    where fornecedor_nome = 'Blessing'
  ),

  exames_laboratoriais as (
    select
      cast(null as string) as id_cnes,
      exame_solicitacao_id as id_solicitacao_fonte,
      id_exame as id_exame_fonte,
      {{ dbt_utils.generate_surrogate_key(['exame_solicitacao_id']) }} as id_solicitacao,
      {{ dbt_utils.generate_surrogate_key(['exame_solicitacao_id', 'id_exame']) }} as id_exame,
      estabelecimento_nome as unidade_nome,
      cast(null as string) as unidade_ap,
      {{ proper_br('paciente_nome') }} as paciente_nome,
      paciente_nascimento_data,
      paciente_cns,
      paciente_cpf,
      lower(paciente_sexo) as paciente_sexo,
      exame_coleta_data_hora as exame_coleta_datahora,
      cast(exame_data as date) as exame_data,
      exame_nome,
      cast(null as string) as exame_codigo,
      exame_tipo,
      exame_metodo,
      exame_laudo_descricao,
      exame_resultado_laudo as exame_laudo,
      lower(exame_status) as exame_status,
      exame_laudo_data_hora as exame_laudo_datahora,
      array(
        select as struct
          cast(null as string) as codigo,
          item.nome as nome,
          cast(item.valor as string) as valor,
          item.unidade as unidade,
          cast(null as string) as alterado,
          item.referencia as referencia
        from unnest(exame_resultado_valor) as item
      ) as resultados,
      profissional_solicitante_cbo,
      profissional_solicitante_cpf,
      profissional_solicitante_crm,
      {{ proper_br('profissional_solicitante_nome') }} as profissional_solicitante_nome,
      profissional_laudista_cbo as profesional_laudista_cbo,
      profissional_laudista_cpf,
      {{ proper_br('profissional_laudista_nome') }} as profissional_laudista_nome,
      loaded_at,
      data_particao
    from blessing
  )

select * from exames_laboratoriais
