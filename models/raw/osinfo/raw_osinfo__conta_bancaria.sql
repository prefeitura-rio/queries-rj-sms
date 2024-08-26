{{
    config(
        alias="conta bancaria",
    )
}}

-- source:
with
    banco as (select * from {{ source("osinfo", "banco") }}),
    agencia as (select * from {{ source("osinfo", "agencia") }}),
    conta_bancaria as (select * from {{ source("osinfo", "conta_bancaria") }}),
    conta_bancaria_tipo as (select * from {{ source("osinfo", "conta_bancaria_tipo") }})

select
    cb.id_conta_bancaria as id,
    b.id_banco,
    b.cod_banco as banco_codigo,
    b.banco as banco_nome,
    b.nome_fantasia as banco_nome_fantasia,
    b.flg_ativo as banco_ativo_indicador,
    a.id_agencia,
    a.numero_agencia as agencia_numero,
    a.digito as agencia_digito,
    a.agencia as agencia_nome,
    a.flg_ativo as agencia_ativo_indicaodr,
    cb.codigo_cc as codigo,
    cb.digito_cc as digito,
    cb.cod_instituicao as instituicao_codigo,
    cb.flg_ativo as ativo_indicador,
    cbt.tipo as tipo,
from banco b
inner join agencia a on b.id_banco = a.id_banco
inner join conta_bancaria cb on a.id_agencia = cb.id_agencia
inner join conta_bancaria_tipo cbt on cb.cod_tipo = cbt.id_conta_bancaria_tipo
