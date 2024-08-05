{{
    config(
        alias="saldos",
    )
}}

with
    saldo_dados as (select * from {{ source("osinfo", "saldo_dados") }}),
    saldo_item as (select * from {{ source("osinfo", "saldo_item") }}),
    contrato as (select * from {{ source("osinfo", "contrato") }})

-- --- Saldos
select
    s.id_saldo_dados as saldoid,
    s.id_saldo_item as saldoitemid,
    si.saldo_item as saldoitem,
    s.referencia_mes_receita as mesreferenciareceita,
    s.referencia_ano_receita as anoreferenciareceita,
    s.valor as valor,
    s.id_instrumento_contratual as contratoid,
    c.numero_contrato as numerocontrato,
    c.cod_organizacao as codigoorganizacao,
    s.id_conta_bancaria as contabancariaid,
    s.arq_img_ext as imagemextrato,
from saldo_dados s
inner join saldo_item si on s.id_saldo_item = si.id_saldo_item
inner join contrato c on s.id_instrumento_contratual = c.id_contrato
