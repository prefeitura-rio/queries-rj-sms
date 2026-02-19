{{
    config(
        schema="brutos_siclom_api",
        alias="pep",
        tags=["siclom"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

-- TODO: Confirmar nome das colunas com a SAP

with 
    source as (select * from {{ source('brutos_siclom_api_staging', 'pep') }})

select
  {{ process_null('CPF') }} as cpf,
  {{ process_null('NOME_CIVIL') }} as paciente_nome,
  {{ process_null('NOME_SOCIAL') }} as paciente_nome_social,
  {{ process_null('SITUACAO_ESTRANGEIRO') }} as situacao_estrangeiro,
  {{ process_null('st_habitantes_fronteira') }} as habitante_fronteira,
  {{ process_null('IDENTIDADE_DE_GENERO') }} as identidade_genero,
  {{ process_null('SITUACAO_DE_RUA') }} as situacao_rua,
  {{ process_null('PRIVADA_DE_LIBERDADE') }} as privada_liberdade,
  {{ process_null('DATA_DA_EXPOSICAO') }} as exposicao_data,
  {{ process_null('CIRCUNSTANCIA_DA_EXPOSICAO') }} as circunstancia_exposicao,
  {{ process_null('ORIGEM_DO_ACOMPANHAMENTO') }} as origem_acompanhamento,
  {{ process_null('SERVICO_DE_ATENDIMENTO') }} as servico_atendimento,
  {{ process_null('UF_DISPENSADOR') }} as uf_dispensador,
  {{ process_null('DISPENSADOR') }} as dispensador,
  {{ process_null('PESSOA_FONTE') }} as pessoa_fonte,
  {{ process_null('st_contraindicacao_esquema') }} as contraindicacao_esquema,
  {{ process_null('DATA_DA_DISPENSA') }} as dispensa_data,
  {{ process_null('ESQUEMA') }} as esquema,
  {{ process_null('DURACAO') }} as duracao,
  {{ process_null('TP_PROFISSIONAL') }} as profissional_tipo,
  {{ process_null('DATA_DA_PRESCRICAO') }} as prescricao_data,
  {{ process_null('tp_testagem_hiv') }} as testagem_hiv,
  {{ process_null('st_esquema_alternativo') }} as esquema_alternativo,
  {{ process_null('extracted_at') }} as extraido_em,
  date(data_particao) as data_particao
from source 