{{
  config(
    schema="exemplo",
    alias="num_agendamentos_vitacare",
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day",
    },
  )
}}

with
  source as (
    select

      count(distinct id_agendamento) as qtd_agendamentos,
      data_particao

    from {{ ref("raw_prontuario_vitacare_api__agendamento") }}
    {% if is_incremental() %}
      where data_particao >= date_sub(
        current_date("America/Sao_Paulo"),
        interval 5 day
      )
    {% endif %}
    group by data_particao
  )

select *
from source




-- Aqui usamos incremental_strategy="insert_overwrite",
-- Nessa estratégia, quando o modelo termina de rodar,
-- todas as partições no resultado substituem as partições
-- existentes no BigQuery

-- Também existe incremental_strategy="merge",
-- "merge" requer um lista de chaves primárias no config(),
-- p.ex.: unique_key=['id_vacinacao']
-- Quando o modelo termina de rodar, ele busca todas as linhas
-- com a mesma chave no BigQuery, e substitui elas pelos
-- dados novos.

-- /!\ Se você não for esperto sobre particionamento,
-- isso pode custar até mais do que só materializar a
-- tabela inteira, porque ele vai fazer 1 consulta nova
-- por chave a ser atualizada!
