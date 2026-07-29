{{
    config(
        alias="ficha_c",
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
ficha_c as (
  select 
    competencia,
    nome_pessoa_cadastrada,
    data_cadastro,
    cpf,
    cpf_valido,
    data_nascimento,
    obito,
    perfil_bpc,
    afastada_da_escola_por_motivo_saude,
    vacinas_em_dia,
    estado_nutricional,
    atraso_desenvolvimento,
    sinais_risco,
    ano_particao,
    mes_particao,
    data_particao,
    metadados
  from {{ ref('raw_informes_vitacare__ficha_c_v2') }}
)

select
    cpf,
    cpf_valido,
    competencia,
    data_cadastro,
    nome_pessoa_cadastrada as nome,
    data_nascimento,
    obito,
    perfil_bpc,
    afastada_da_escola_por_motivo_saude,
    vacinas_em_dia,
    estado_nutricional,
    atraso_desenvolvimento,
    sinais_risco,
    ano_particao,
    mes_particao,
    data_particao,
    
    -- Metadados
    metadados.criado_em as criado_em,
    current_datetime('America/Sao_Paulo') as processado_em,

    safe_cast(cpf as int64) as cpf_particao
from ficha_c
