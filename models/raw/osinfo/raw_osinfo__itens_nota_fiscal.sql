{{
    config(
        alias="itens_nota_fiscal",
    )
}}


with
    itens_nota_fiscal as (select * from {{ source("osinfo", "itens_nota_fiscal") }}),
    fornecedor as (select * from {{ source("osinfo", "fornecedor") }})

select
    nf.id_item_nf as itemnfid,
    nf.cod_item_nf as codigoitemnf,
    nf.qtd_material as quantidadematerial,
    nf.valor_unitario as valorunitario,
    nf.referencia_mes_nf as mesreferencianf,
    nf.referencia_ano_nf as anoreferencianf,
    nf.id_fornecedor as fornecedorid,
    f.cod_fornecedor as codigofornecedor,
    f.fornecedor as nomefornecedor,
    f.tipo_pessoa as tipopessoa,
    nf.valor_total as valortotal,
    nf.num_documento as numerodocumento,
    nf.cod_instituicao as codigoinstituicao,
    nf.item as itemdescricao,
    nf.unidade_medida as unidademedida
from itens_nota_fiscal nf
inner join fornecedor f on nf.id_fornecedor = f.id_fornecedor
