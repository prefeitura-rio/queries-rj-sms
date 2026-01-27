{{
    config(
        schema="brutos_siclom_api",
        alias="resultado_prep",
        tags=["siclom"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}

with source as (select * from {{ source('brutos_siclom_api_staging', 'resultadoprep') }})

select  
  {{ process_null('udm') }} as unidade_nome,
  {{ process_null('municipio_udm') }} as unidade_municipio,
  {{ process_null('uf_udm') }} as unidade_uf,
  {{ process_null('tp_servico_atendimento') }} as servico_atendimento,
  {{ process_null('st_pub_priv') }} as tipo_atendimento,
  {{ process_null('CPF') }} as cpf,
  {{ process_null('nome_civil') }} as paciente_nome,
  {{ process_null('nome_social') }} as paciente_nome_social,
  {{ process_null('ds_genero') }} as genero,
  {{ process_null('st_participante_estudo_vacina') }} as participante_estudo_vacina,
  {{ process_null('st_dinheiro_sexo') }} as dinheiro_sexo,
  {{ process_null('st_droga_injetavel') }} as droga_injetavel,
  {{ process_null('st_substancias_psicoativias') }} as substancias_psicoativas,
  {{ process_null('tp_modalidade') }} as modalidade,
  {{ process_null('tp_esquema_prep') }} as esquema_prep,
  {{ process_null('qtde_autoteste') }} as quantidade_autoteste,
  {{ process_null('duracao') }} as duracao,
  {{ process_null('dt_dispensa_sol') }} as data_dispensa,
  {{ process_null('resultado_ist') }} as resultado_ist,
  {{ process_null('extracted_at') }} as extraido_em,
  {{ process_null('data_particao') }} as data_particao,
  cast(cpf as int64) as cpf_particao
from source