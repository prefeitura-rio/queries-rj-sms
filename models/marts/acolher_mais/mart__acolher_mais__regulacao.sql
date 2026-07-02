{{
    config(
        alias="regulacoes",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}


with

dados as (
  select
    cast(paciente_cpf as int64) as cpf_particao,
    id_solicitacao,

    solicitacao_status, 
    solicitacao_risco,

    paciente_cpf,

    data_solicitacao,     
    data_autorizacao,
    data_execucao,
    data_confirmacao,       
    data_cancelamento,

    cancelamento_autor,
    justificativa_cancelamento,

    procedimento_grupo,
    procedimento,

    id_cnes_unidade_solicitante,
    unidade_executante,

    profissional_solicitante_nome,
  from {{ref('mart_sisreg__solicitacoes')}}
  where cast(data_solicitacao as date) > date_sub(current_date(), interval 1 year)
),

enriquecido as (
  select 
    d.*, 
    e.nome_limpo as unidade_solicitante
  from dados as d
    inner join {{ref('dim_estabelecimento')}} e on e.id_cnes = d.id_cnes_unidade_solicitante
)

select * 
from enriquecido
