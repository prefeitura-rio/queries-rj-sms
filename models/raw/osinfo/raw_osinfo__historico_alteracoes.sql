{{
    config(
        alias="historico_alteracoes",
    )
}}


with
    historico_alteracoes as (
        select * from {{ source("brutos_osinfo_staging", "historico_alteracoes") }}
    ),
    usuario as (select * from {{ source("brutos_osinfo_staging", "usuario") }})

select
    ha.id_historico_alteracoes as id,
    ha.id_tipo_arquivo,
    ha.cod_organizacao as id_organizacao,
    ha.data_modificacao as modificacao_data,
    ha.valor_anterior,
    ha.valor_novo,
    ha.mes_referencia as referencia_mes,
    ha.ano_referencia as referencia_ano,
    ha.id_registro,
    ha.tipo_alteracao as alteracao_tipo,
    ha.cod_usuario as usuario_cod,
    u.login as usuario_login,
    u.nome as usuario_nome,
from historico_alteracoes ha
inner join usuario u on ha.cod_usuario = u.cod_usuario
