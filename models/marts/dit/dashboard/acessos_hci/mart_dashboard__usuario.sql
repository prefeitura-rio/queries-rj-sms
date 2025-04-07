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
    from {{ref('int_acessos__automatico')}}
    union all 
    select *
    from {{ref('int_acessos__manual')}}
)

select
  historico_acessos.*, 
  nome as nome_usuario, 
  dim_usuario.cpf as cpf_usuario,
  indicador_ativo,
  indicador_superusuario,
  ap as area_programatica,
  profissional.unidade_nome,
  cnes as cnes_lotacao,
  data_aceite_termos_uso,
  indicador_aceite_termos_uso,
  nivel_acesso,
  cargo,
  profissional.funcao_grupo
from historico_acessos
left join dim_usuario
on cast(historico_acessos.id_usuario as string) = cast(dim_usuario.id as string)
left join profissional
on cast(dim_usuario.cpf as string) = cast(profissional.cpf as string)