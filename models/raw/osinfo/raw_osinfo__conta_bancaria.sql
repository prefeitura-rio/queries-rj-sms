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
    b.id_banco as bancoid,
    b.cod_banco as codigobanco,
    b.banco as nomebanco,
    b.nome_fantasia as nomefantasia,
    b.flg_ativo as bancoativo,
    a.id_agencia as agenciaid,
    a.numero_agencia as numeroagencia,
    a.digito as digitoagencia,
    a.agencia as nomeagencia,
    a.flg_ativo as agenciaativo,
    cb.codigo_cc as codigocontacorrente,
    cb.digito_cc as digitocontacorrente,
    cb.cod_instituicao as codigoinstituicao,
    cb.flg_ativo as contaativa,
    cbt.tipo as tipoconta
from banco b
inner join agencia a on b.id_banco = a.id_banco
inner join conta_bancaria cb on a.id_agencia = cb.id_agencia
inner join conta_bancaria_tipo cbt on cb.cod_tipo = cbt.id_conta_bancaria_tipo
