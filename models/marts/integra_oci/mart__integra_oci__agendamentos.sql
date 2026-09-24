{{
    config(
        alias="agendamentos",
        materialized="table"
    )
}}


with

dados as (
  select
    paciente_cpf,
    paciente_cns,
    procedimento.sigtap_id,
    solicitante
  from {{ref('mart_regulacao__solicitacao')}} s
  where 
    cancelamento.datahora is null and 
    solicitacao.solicitacao_datahora > '2026-09-01' and
    solicitante.unidade_id_cnes in (
        select id_cnes
        from {{ref('dim_estabelecimento')}}
    )
)

select * 
from dados
