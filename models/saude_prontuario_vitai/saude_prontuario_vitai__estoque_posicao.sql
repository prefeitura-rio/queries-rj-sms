{{
    config(
        alias="estoque_posicao",
        schema="saude_prontuario_vitai",
        labels = {'contains_pii': 'no'},
        materialized="incremental",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

select
    -- Primary Key
    id as id_estoque_posicao,

    -- Foreign Keys
    safe_cast(cnes as string) as id_cnes,
    safe_cast(lote as string) as id_lote,
    safe_cast(regexp_replace(produtocodigo, r'[^0-9]', '') as string) as id_produto,

    -- Logical Info
    safe_cast(secao as string) as estoque_secao,
    safe_cast(descricao as string) as produto_descricao,
    safe_cast(apresentacao as string) as produto_unidade,
    safe_cast(datavencimento as datetime) as lote_data_vencimento,
    safe_cast(saldo as float64) as produto_quantidade,
    safe_cast(valormedio as float64) as produto_valor_unitario,
    safe_cast(valormedio as float64)
    * safe_cast(saldo as float64) as produto_valor_total,

    -- metadata
    safe_cast(data_particao as date) as data_particao,
    safe_cast(datahora as datetime) as data_snapshot,
    safe_cast(_data_carga as date) as data_carga,

from `rj-sms-dev.saude_prontuario_vitai_staging.estoque_posicao`
where
    safe_cast(data_particao as date) <= current_date('America/Sao_Paulo')

    {% if is_incremental() %}

        {% set max_partition = (
            run_query(
                "SELECT gr FROM (SELECT IF(max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)) as gr FROM "
                ~ this
                ~ ")"
            )
            .columns[0]
            .values()[0]
        ) %}

        and safe_cast(data_particao as date) > ("{{ max_partition }}")

    {% endif %}
