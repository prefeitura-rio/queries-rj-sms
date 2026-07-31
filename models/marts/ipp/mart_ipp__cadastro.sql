{{
    config(
        alias="cadastro",
        schema="projeto_ipp",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with 

paciente as (
  select 
    cpf,
    cns,
    dados,
    cpf_particao,
    contato,
    endereco 
  from {{ ref('mart_historico_clinico__paciente') }}
)

select
  cpf,
  cns,
  dados.nome,
  dados.nome_social,
  dados.data_nascimento,
  dados.genero,
  dados.raca,
  dados.obito_indicador,
  dados.obito_data,
  dados.mae_nome,
  dados.pai_nome,
  dados.identidade_validada_indicador,
  dados.cpf_valido_indicador,
  contato.telefone,
  
  -- Metadados
  current_datetime('America/Sao_Paulo') as processado_em,
  
  cpf_particao,



from paciente 