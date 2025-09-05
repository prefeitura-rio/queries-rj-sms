{{
    config(
        alias="despesas",
    )
}}

with
    despesas as (select * from {{ source("brutos_osinfo_staging", "despesas") }}),
    contrato as (select * from {{ source("brutos_osinfo_staging", "contrato") }}),
    plano_contas as (
        select * from {{ source("brutos_osinfo_staging", "plano_contas") }}
    ),
    rubrica as (select * from {{ source("brutos_osinfo_staging", "rubrica") }}),
    administracao_unidade as (
        select * from {{ source("brutos_osinfo_staging", "administracao_unidade") }}
    ),
    conta_bancaria as (
        select * from {{ source("brutos_osinfo_staging", "conta_bancaria") }}
    ),
    conta_bancaria_tipo as (
        select * from {{ source("brutos_osinfo_staging", "conta_bancaria_tipo") }}
    ),
    agencia as (select * from {{ source("brutos_osinfo_staging", "agencia") }}),
    banco as (select * from {{ source("brutos_osinfo_staging", "banco") }}),
    tipo_documento as (
        select * from {{ source("brutos_osinfo_staging", "tipo_documento") }}
    )

select
    d.id_contrato,
    c.numero_contrato as contrato_numero,
    d.cod_organizacao as id_organizacao,
    d.cod_unidade as id_unidade,
    au.nome_fantasia as unidade_nome_fantasia,
    d.referencia_mes,
    d.referencia_ano,
    d.cod_bancario as codigo_bancario,
    d.id_conta_bancaria,
    cb.codigo_cc as conta_bancaria_codigo,
    cb.digito_cc as conta_bancaria_digito,
    cb.id_agencia,
    a.numero_agencia as agencia_numero,
    a.digito as agencia_digito,
    b.cod_banco as banco_cod,
    b.banco as banco_nome,
    cbt.tipo as conta_bancaria_tipo,
    d.cnpj as contratada_cnpj,
    d.razao as contratada_razaosocial,
    d.cpf as colaborador_cpf,
    d.nome as colaborar_nome,  -- # TODO: confirmar que o nome se refere ao colaborador
    d.num_documento as documento_pago_numero,
    td.tipo_documento as documento_pago_tipo,
    d.serie as documento_pago_serie,
    d.descricao as documento_pago_descricao,  -- TODO: confirmar descrição do campo
    d.data_emissao as documento_emissao_data,
    d.data_vencimento as documento_vencimento_data,
    d.data_pagamento as documento_pagamento_data,
    d.data_apuracao as documento_apuracao_data,
    d.valor_documento as documento_valor,
    d.valor_pago as documento_valor_pago,
    pc.cod_item_plano_de_contas as plano_contas_cod,
    pc.descricao_item_plano_de_contas as plano_contas_descricao,
    d.id_rubrica as id_rubrica,
    r.rubrica as rubrica_descricao,
    d.parcela_mes,
    d.parcelamento_total as parcela_total,
    d.nf_validada_sigma as nota_fiscal_validada_sigma,
    d.data_validacao as validacao_data
from despesas d
inner join contrato c on d.id_contrato = c.id_contrato
inner join plano_contas pc on d.id_despesa = pc.id_item_plano_de_contas
inner join rubrica r on d.id_rubrica = r.id_rubrica
inner join administracao_unidade au on d.cod_unidade = au.cod_unidade
inner join conta_bancaria cb on d.id_conta_bancaria = cb.id_conta_bancaria
inner join conta_bancaria_tipo cbt on cb.cod_tipo = cbt.id_conta_bancaria_tipo
inner join agencia a on cb.id_agencia = a.id_agencia
inner join banco b on a.id_banco = b.id_banco
inner join tipo_documento td on d.id_tipo_documento = td.id_tipo_documento
where c.id_secretaria = '1'
