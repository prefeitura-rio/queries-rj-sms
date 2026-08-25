{{
    config(
        alias="gestante_acompanhamento_mensal",
        schema="projeto_ipp",
        materialized="table",
        partition_by={
            'field': 'cpf_particao',
            'data_type': 'int64',
            'range': {'start': 0, 'end': 100000000000, 'interval': 34722222},
        }
    )
}}

with 
  acompanhamento_gestante as (
    select  
      id_surrogate,
      nome,
      data_de_nascimento,
      nome_mae,
      cpf,
      cpf_valido,
      semana_gestacional_na_1a_cons_de_pre_natal,
      data_da_ult_cons_pre_natal,
      risco_gestacional,
      periodo_extracao_inicio,
      periodo_extracao_fim,
      periodo_extracao,
      metadados
    from {{ ref('raw_informes_vitacare__acompanhamento_mensal_gestantes') }}
  )

select
    id_surrogate as id,
    cpf,
    cpf_valido,
    nome,
    data_de_nascimento as data_nascimento,
    nome_mae as mae_nome,
    semana_gestacional_na_1a_cons_de_pre_natal as semana_gestacional_1o_pre_natal,
    data_da_ult_cons_pre_natal as data_ultimo_pre_natal,
    risco_gestacional,
    periodo_extracao_inicio,
    periodo_extracao_fim,
    periodo_extracao,
    
    -- Metadados
    datetime(metadados.criado_em, 'America/Sao_Paulo') as criado_em,
    datetime(metadados.carregado_em, 'America/Sao_Paulo') as carregado_em,
    current_datetime('America/Sao_Paulo') as processado_em,

    safe_cast(cpf as int64) as cpf_particao    
from acompanhamento_gestante