{{ config(alias='estoque_posicao', schema='saude_prontuario_vitai') }}

select
    cnes as id_cnes,
    regexp_replace(produtocodigo, r'[^0-9]', '') as id_produto,
    descricao,
    apresentacao,
    lote id_lote,
    cast(datavencimento as datetime) as vencimento_data,
    date_diff(cast(datavencimento as datetime), current_date(), day) as vencimento_dias,
    cast(saldo as float64) as qtd,
    cast(valormedio as float64) as custo_unitario,
    cast(valormedio as float64) * cast(saldo as float64) as custo_total,
    ano_particao,
    mes_particao,
    data_particao,
from `rj-sms-dev.dump_vitai_staging.estoque_posicao`
where
    _data_carga
    = (select max(_data_carga) from `rj-sms-dev.dump_vitai_staging.estoque_posicao`)
