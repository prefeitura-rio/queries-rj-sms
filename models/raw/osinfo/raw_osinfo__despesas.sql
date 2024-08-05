{{
    config(
        alias="despesas",
    )
}}

with
    despesas as (select * from {{ source("osinfo", "despesas") }}),
    contrato as (select * from {{ source("osinfo", "contrato") }}),
    plano_contas as (select * from {{ source("osinfo", "plano_contas") }}),
    rubrica as (select * from {{ source("osinfo", "rubrica") }}),
    administracao_unidade as (
        select * from {{ source("osinfo", "administracao_unidade") }}
    ),
    conta_bancaria as (select * from {{ source("osinfo", "conta_bancaria") }}),
    conta_bancaria_tipo as (
        select * from {{ source("osinfo", "conta_bancaria_tipo") }}
    ),
    agencia as (select * from {{ source("osinfo", "agencia") }}),
    banco as (select * from {{ source("osinfo", "banco") }}),
    tipo_documento as (select * from {{ source("osinfo", "tipo_documento") }})

select
    d.id_contrato as contratoid,
    c.numero_contrato as numerocontrato,
    d.cod_organizacao as codigoorganizacao,
    d.cod_unidade as codigounidade,
    au.nome_fantasia as unidadenomefantasia,
    d.referencia_mes as mesreferencia,
    d.referencia_ano as anoreferencia,
    d.cod_bancario as codigobancario,
    d.id_conta_bancaria as contabancariaid,
    cb.codigo_cc as codigocontacorrente,
    cb.digito_cc as digitocontacorrente,
    cb.id_agencia as agenciaid,
    a.numero_agencia as numeroagencia,
    a.digito as digitoagencia,
    b.cod_banco as codigobanco,
    b.banco as nomebanco,
    cbt.tipo as tipoconta,
    d.cnpj as cnpj,
    d.razao as razaosocial,
    d.cpf as cpf,
    d.nome as nome,
    d.num_documento as numerodocumento,
    td.tipo_documento as tipodocumento,
    d.serie as seriedocumento,
    d.descricao as descricao,
    d.data_emissao as dataemissao,
    d.data_vencimento as datavencimento,
    d.data_pagamento as datapagamento,
    d.data_apuracao as dataapuracao,
    d.valor_documento as valordocumento,
    d.valor_pago as valorpago,
    pc.cod_item_plano_de_contas as codigoplanocontas,
    pc.descricao_item_plano_de_contas as descricaoplanocontas,
    d.id_rubrica as rubricaid,
    r.rubrica as rubrica,
    d.parcela_mes as parcelames,
    d.parcelamento_total as parcelamentototal,
    d.nf_validada_sigma as nfvalidadasigma,
    d.data_validacao as datavalidacao
from despesas d
inner join contrato c on d.id_contrato = c.id_contrato
inner join plano_contas pc on d.id_despesa = pc.id_item_plano_de_contas
inner join rubrica r on d.id_rubrica = r.id_rubrica
inner join administracao_unidade au on d.cod_unidade = au.cod_unidade
inner join conta_bancaria cb on d.id_conta_bancaria = cb.id_conta_bancaria
inner join
    conta_bancaria_tipo cbt on cb.cod_tipo = cbt.id_conta_bancaria_tipo
inner join agencia a on cb.id_agencia = a.id_agencia
inner join banco b on a.id_banco = b.id_banco
inner join tipo_documento td on d.id_tipo_documento = td.id_tipo_documento
where c.id_secretaria = '1'
