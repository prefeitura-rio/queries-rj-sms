{{
    config(
        alias="diario_uniao_filtrado",
        materialized="table",
    )
}}

-- Monta tabela base para envio de emails CDI

with filtros_diario_uniao as (
    select
        * 
    from {{ref('raw_diario_oficial__diarios_uniao_api')}}
    where (
        lower(texto)  like "%município do rio de janeiro%"
        or lower(organizacao_principal) like "%município do rio de janeiro%"
    )
    and (
        lower(texto) like "hospital municipal%"
        or texto like "CER %"
        or texto like "UPA %"
        or texto like "CF %"
        or texto like "CMS %"
        or texto like "Policlínica %"
        or lower(texto)  like "clínica% da família%"
        or texto like "%Hospital Federal Cardoso Fontes%"
        or texto like "%Hospital Federal de Andaraí%"
        or lower(texto)  like "%saúde%"
        or texto like "%Daniel Ricardo Soranz Pinto%"
        or texto like "%Daniel Soranz%"
        or texto like "%Rodrigo Sousa Prado%"
        or texto like "%Rodrigo Prado%"
    )
),
titulos_diario_uniao as (
    select distinct id_oficio, texto_titulo
    from {{ref('raw_diario_oficial__diarios_uniao_api')}}
    where texto_titulo is not null
)

select distinct * except(texto_titulo),
        upper(titulos_diario_uniao.texto_titulo) as content_email
from filtros_diario_uniao
left join titulos_diario_uniao
using (id_oficio)
