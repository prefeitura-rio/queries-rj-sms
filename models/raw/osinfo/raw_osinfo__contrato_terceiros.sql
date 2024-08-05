{{
    config(
        alias="contrato_terceiros",
    )
}}

with
    contrato_terceiros as (select * from {{ source("osinfo", "contrato_terceiros") }}),
    contrato as (select * from {{ source("osinfo", "contrato") }})

select
    ct.id_contrato_terceiro as contratoterceiroid,
    ct.cod_organizacao as codigoorganizacao,
    ct.cod_unidade as codigounidade,
    ct.id_instrumento_contratual as instrumentocontratualid,
    c.numero_contrato as numerocontrato,
    ct.valor_mes as valormensal,
    ct.contrato_mes_inicio as mesiniciocontrato,
    ct.contrato_mes_fim as mesfimcontrato,
    ct.contrato_ano_inicio as anoiniciocontrato,
    ct.contrato_ano_fim as anofimcontrato,
    ct.referencia_ano_ass_contrato as anoassinaturacontrato,
    ct.vigencia as vigencia,
    ct.cnpj as cnpj,
    ct.razao_social as razaosocial,
    ct.servico as servico,
    ct.imagem_contrato as imagemcontrato
from contrato_terceiros ct
inner join contrato c on ct.id_instrumento_contratual = c.id_contrato
