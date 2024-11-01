{{
    config(
        alias="contrato_terceiros",
    )
}}

with
    contrato_terceiros as (
        select * from {{ source("brutos_osinfo_staging", "contrato_terceiros") }}
    ),
    contrato as (select * from {{ source("brutos_osinfo_staging", "contrato") }})

select
    ct.id_contrato_terceiro,
    ct.cod_organizacao as id_organizacao,
    ct.cod_unidade as id_unidade,
    ct.id_instrumento_contratual,
    c.numero_contrato as numero,
    ct.valor_mes as valor_mensal,
    ct.contrato_mes_inicio as contato_inicio_mes,
    ct.contrato_mes_fim as contrato_fim_mes,
    ct.contrato_ano_inicio as contrato_inicio_ano,
    ct.contrato_ano_fim as contrato_fim_ano,
    ct.referencia_ano_ass_contrato as contrato_assinatura_ano,
    ct.vigencia as vigencia,
    ct.cnpj as cnpj,
    ct.razao_social,
    ct.servico as servico,
    ct.imagem_contrato
from contrato_terceiros ct
inner join contrato c on ct.id_instrumento_contratual = c.id_contrato
