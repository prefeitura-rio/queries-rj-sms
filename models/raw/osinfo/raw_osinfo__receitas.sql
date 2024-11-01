{{
    config(
        alias="receitas",
    )
}}

with
    receita_dados as (
        select * from {{ source("brutos_osinfo_staging", "receita_dados") }}
    ),
    contrato as (select * from {{ source("brutos_osinfo_staging", "contrato") }}),
    receita_item as (
        select * from {{ source("brutos_osinfo_staging", "receita_item") }}
    ),
    conta_bancaria as (
        select * from {{ source("brutos_osinfo_staging", "conta_bancaria") }}
    ),
    conta_bancaria_tipo as (
        select * from {{ source("brutos_osinfo_staging", "conta_bancaria_tipo") }}
    ),
    agencia as (select * from {{ source("brutos_osinfo_staging", "agencia") }})

select
    rd.cod_unidade as id_unidade,
    rd.referencia_mes,
    rd.referencia_ano,
    rd.valor,
    c.id_contrato,
    c.numero_contrato as contrato_numero,
    rd.id_conta_bancaria,
    cb.codigo_cc as conta_bancaria_codigo,
    cb.digito_cc as conta_bancaria_digito,
    cbt.tipo as conta_bancaria_tipo,
    a.id_agencia,
    a.agencia as agencia_nome,
    a.numero_agencia as agencia_numero,
    a.digito as agencia_digito,
    a.id_banco,
    ri.receita_item as item
from receita_dados rd
inner join contrato c on rd.id_instrumento_contratual = c.id_contrato
inner join receita_item ri on rd.id_item = ri.id_receita_item
inner join conta_bancaria cb on rd.id_conta_bancaria = cb.id_conta_bancaria
inner join conta_bancaria_tipo cbt on cb.cod_tipo = cbt.id_conta_bancaria_tipo
inner join agencia a on cb.id_agencia = a.id_agencia
where c.id_secretaria = '1'
