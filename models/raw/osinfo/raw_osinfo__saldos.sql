{{
    config(
        alias="saldos",
    )
}}

with
    saldo_dados as (select * from {{ source("brutos_osinfo_staging", "saldo_dados") }}),
    saldo_item as (select * from {{ source("brutos_osinfo_staging", "saldo_item") }}),
    contrato as (select * from {{ source("brutos_osinfo_staging", "contrato") }})

-- --- Saldos
select
    s.id_saldo_dados as id,
    s.id_saldo_item as id_item,
    si.saldo_item as item_nome,
    s.referencia_mes_receita as receita_referencia_mes,
    s.referencia_ano_receita as receita_referencia_ano,
    s.valor as valor,
    s.id_instrumento_contratual as id_contrato,
    c.numero_contrato as contrato_numero,
    c.cod_organizacao as id_organizacao,
    s.id_conta_bancaria,
    s.arq_img_ext as extrato_imagem
from saldo_dados s
inner join saldo_item si on s.id_saldo_item = si.id_saldo_item
inner join contrato c on s.id_instrumento_contratual = c.id_contrato
