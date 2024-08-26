{{
    config(
        alias="contrato",
    )
}}

with source as (select * from {{ source("osinfo", "contrato") }})

select
    id_contrato,
    numero_contrato as numero,
    cod_organizacao as id_organizacao,
    data_atualizacao as atualizacao_data,
    data_assinatura as assinatura_data,
    periodo_vigencia as vigencia_periodo,
    data_publicacao as publicao_data,
    data_inicio as inicio_data,
    valor_total,
    valor_ano1,
    valor_parcelas,
    valor_fixo,
    valor_variavel,
    observacao,
    ap as area_programatica
from source
where id_secretaria = '1'
