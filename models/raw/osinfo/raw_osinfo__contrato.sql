{{
    config(
        alias="contrato",
    )
}}

with source as (select * from {{ source("osinfo", "contrato") }})

select
    id_contrato as contratoid,
    numero_contrato as numerocontrato,
    cod_organizacao as codigoorganizacao,
    data_atualizacao as dataatualizacao,
    data_assinatura as dataassinatura,
    periodo_vigencia as periodovigencia,
    data_publicacao as datapublicacao,
    data_inicio as datainicio,
    valor_total as valortotal,
    valor_ano1 as valorano1,
    valor_parcelas as valorparcelas,
    valor_fixo as valorfixo,
    valor_variavel as valorvariavel,
    observacao as observacao,
    ap as ap
from source
where id_secretaria = '1'
