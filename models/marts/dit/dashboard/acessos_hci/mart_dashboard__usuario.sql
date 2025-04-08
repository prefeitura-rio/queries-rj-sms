{{
    config(
        schema="dashboard_historico_clinico",
        alias="hci_usuario",
        tags="dashboard_tables"
    )
}}
with historico_acessos as (
    select * 
    from {{ref('raw_aplicacao_hci__userhistory')}} 
),
dim_usuario as (
    select * 
    from {{ref('raw_aplicacao_hci__user')}}
),
profissional as (
    select * 
    from {{ref('mart_historico_clinico_app__acessos')}}
),
estabelecimento as (
    select * 
    from {{ref('dim_estabelecimento')}}
)

select
historico_acessos.id,
id_usuario,
dim_usuario.cpf as cpf_usuario,
estabelecimento.area_programatica,
estabelecimento.nome_limpo as unidade_nome,
dim_usuario.cnes as cnes_lotacao,
endereco_latitude,
endereco_longitude,
profissional.funcao_grupo,
cast(historico_acessos.updated_at as date) as updated_at,
cast(historico_acessos.loaded_at as date) as loaded_at,

from historico_acessos
left join dim_usuario
on cast(historico_acessos.id_usuario as string) = cast(dim_usuario.id as string)
left join profissional
on cast(dim_usuario.cpf as string) = cast(profissional.cpf as string)
left join estabelecimento
on dim_usuario.cnes = estabelecimento.id_cnes