{{
    config(
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
)

select
  historico_acessos.*, 
  nome as nome_usuario, 
  usuario.cpf as cpf_usuario,
  indicador_ativo,
  indicador_superusuario,
  ap,
  cnes as cnes_lotacao,
  data_aceite_termos_uso,
  indicador_aceite_termos_uso,
  nivel_acesso,
  cargo, 
from historico_acessos
left join dim_usuario
on cast(historico_acessos.id_usuario as string) = cast(dim_usuario.id as string)