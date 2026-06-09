{{
  config(
    schema="regulacao",
    alias="solicitacao",
    materialized="table",
    partition_by={
      "field": "cpf_particao",
      "data_type": "int64",
      "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
  )
}}

with
  solicitacao_sisreg as (
    select
      * except (_run_id),

      -- Marcação
      struct(
        cast(null as string) as id,
        cast(null as datetime) as datahora,
        cast(null as datetime) as aprovacao_datahora,
        cast(null as date) as confirmacao_data,
        cast(null as string) as flag_paciente_avisado,
        cast(null as string) as flag_executada,
        cast(null as string) as flag_falta_registrada
      ) as marcacao,
      -- Execução
      struct(
        cast(null as string) as profissional_crm,
        cast(null as string) as profissional_cpf,
        cast(null as string) as profissional_nome,
        cast(null as string) as unidade_id_cnes,
        cast(null as string) as unidade_nome,
        cast(null as string) as unidade_telefone,
        cast(null as string) as unidade_cep,
        cast(null as string) as unidade_municipio,
        cast(null as string) as unidade_bairro,
        cast(null as string) as unidade_logradouro,
        cast(null as string) as unidade_numero,
        cast(null as string) as unidade_complemento
      ) as execucao,

      "sisreg" as fonte,
      2 as rank
    from {{ ref("int_regulacao__solicitacao_sisreg") }}
  ),
  marcacao_sisreg as (
    select
      * except(marcacao, execucao, _run_id),

      marcacao,
      execucao,

      "sisreg" as fonte,
      1 as rank
    from {{ ref("int_regulacao__marcacao_sisreg") }}
  ),

  joined as (
    select * except (rank)
    from (
      select * from solicitacao_sisreg
      union all
      select * from marcacao_sisreg
    )
    qualify row_number() over (
      partition by solicitacao.id
      order by
        rank asc,
        _extracted_at desc nulls last
    ) = 1
  ),

  particionado as (
    select
      * except (data_particao, tipo, _extracted_at),

      tipo,
      _extracted_at,
      cast(paciente_cpf as int64) as cpf_particao
    from joined
  )

select *
from particionado
