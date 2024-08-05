{{
    config(
        alias="receitas",
    )
}}

with
    receita_dados as (select * from {{ source("osinfo", "receita_dados") }}),
    contrato as (select * from {{ source("osinfo", "contrato") }}),
    receita_item as (select * from {{ source("osinfo", "receita_item") }}),
    conta_bancaria as (select * from {{ source("osinfo", "conta_bancaria") }}),
    conta_bancaria_tipo as (
        select * from {{ source("osinfo", "conta_bancaria_tipo") }}
    ),
    agencia as (select * from {{ source("osinfo", "agencia") }})

select
    rd.cod_unidade as unidadecodigo,
    rd.referencia_mes as mesreferencia,
    rd.referencia_ano as anoreferencia,
    rd.valor as valorreceita,
    c.numero_contrato as numerocontrato,
    c.id_contrato as contratoid,
    rd.id_conta_bancaria as contabancariaid,
    cb.codigo_cc as codigocontacorrente,
    cb.digito_cc as digitocontacorrente,
    cbt.tipo as tipoconta,
    a.id_agencia as agenciaid,
    a.agencia as nomeagencia,
    a.numero_agencia as numeroagencia,
    a.digito as digitoagencia,
    a.id_banco as bancoid,
    ri.receita_item as itemreceita
from receita_dados rd
inner join contrato c on rd.id_instrumento_contratual = c.id_contrato
inner join receita_item ri on rd.id_item = ri.id_receita_item
inner join
    conta_bancaria cb on rd.id_conta_bancaria = cb.id_conta_bancaria
inner join
    conta_bancaria_tipo cbt on cb.cod_tipo = cbt.id_conta_bancaria_tipo
inner join agencia a on cb.id_agencia = a.id_agencia
where c.id_secretaria = '1'
