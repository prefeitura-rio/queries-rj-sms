{{
    config(
        alias="itens_nota_fiscal",
    )
}}


with
    itens_nota_fiscal as (
        select * from {{ source("brutos_osinfo_staging", "itens_nota_fiscal") }}
    ),
    fornecedor as (select * from {{ source("brutos_osinfo_staging", "fornecedor") }})

select
    nf.id_item_nf as id,
    nf.cod_item_nf as codigo,
    nf.qtd_material as material_qtd,
    nf.valor_unitario,
    nf.referencia_mes_nf as referencia_mes,
    nf.referencia_ano_nf as referencia_ano,
    nf.id_fornecedor,
    f.cod_fornecedor as fornecedor_cnpj,
    f.fornecedor as fornecedor_nome,
    f.tipo_pessoa as pessoa_tipo,
    nf.valor_total,
    nf.num_documento as documento_numero,
    nf.cod_instituicao as id_instituicao,
    nf.item as descricao,
    nf.unidade_medida as unidade_medida
from itens_nota_fiscal nf
inner join fornecedor f on nf.id_fornecedor = f.id_fornecedor
