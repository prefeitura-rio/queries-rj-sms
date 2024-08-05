{{
    config(
        alias="historico_alteracoes",
    )
}}


with
    historico_alteracoes as (
        select * from {{ source("osinfo", "historico_alteracoes") }}
    ),
    usuario as (select * from {{ source("osinfo", "usuario") }})

select
    ha.id_historico_alteracoes as historicoalteracoesid,
    ha.id_tipo_arquivo as tipoarquivoid,
    ha.cod_organizacao as codigoorganizacao,
    ha.data_modificacao as datamodificacao,
    ha.valor_anterior as valoranterior,
    ha.valor_novo as valornovo,
    ha.mes_referencia as mesreferencia,
    ha.ano_referencia as anoreferencia,
    ha.id_registro as registroid,
    ha.tipo_alteracao as tipoalteracao,
    ha.cod_usuario as codigousuario,
    u.login as loginusuario,
    u.nome as nomeusuario,
from historico_alteracoes ha
inner join usuario u on ha.cod_usuario = u.cod_usuario
