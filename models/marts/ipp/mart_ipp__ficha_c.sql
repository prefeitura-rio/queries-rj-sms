{{
    config(
        alias="ficha_c",
        schema="projeto_ipp",
        materialized="incremental",
        description='Tabela contendo os atendimentos da atenção primária do município do Rio de Janeiro. Dados oriundos do prontuário Vitacare'
    )
}}

-- TODO: Adiciona particionamento e desenvolver lógica incremental 

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
from ficha_c
