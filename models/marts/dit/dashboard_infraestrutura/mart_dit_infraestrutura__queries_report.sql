with

-- Execucoes entre 6 e 7 da manhã
execucoes_dbt_rotina as (
 select 
  cast(creation_time as date) as dia, 
  sum(billing_estimated_charge_in_usd) * 5.36 as custo_materializacao_do_dia
 from {ref('mart_dit_infraestrutura__queries')}
 where 
  EXTRACT(HOUR FROM creation_time) in (9) and 
  user.email like 'prefect%'
 group by 1
),

execucoes_dbt_correcoes as (
 select 
  cast(creation_time as date) as dia, 
  sum(billing_estimated_charge_in_usd) * 5.36 as custo_materializacao_do_dia
 from {ref('mart_dit_infraestrutura__queries')}
 where
  EXTRACT(HOUR FROM creation_time) not in (9) and 
  user.email like 'prefect%'
 group by 1
),

execucoes_dbt_livre_demanda as (
 select 
  cast(creation_time as date) as dia, 
  sum(billing_estimated_charge_in_usd) * 5.36 as custo_materializacao_do_dia
 from {ref('mart_dit_infraestrutura__queries')}
 where
  EXTRACT(HOUR FROM creation_time) not in (9) and 
  user.email like '%dbt%'
 group by 1
),

execucoes_studio_livre_demanda as (
 select 
  cast(creation_time as date) as dia, 
  sum(billing_estimated_charge_in_usd) * 5.36 as custo_materializacao_do_dia
 from {ref('mart_dit_infraestrutura__queries')}
 where
  user.email not like 'prefect%' and user.email not like '%dbt%'
 group by 1
)

select
  coalesce(execucoes_dbt_rotina.dia, execucoes_dbt_correcoes.dia, execucoes_dbt_livre_demanda.dia, execucoes_studio_livre_demanda.dia) as dia,
  execucoes_dbt_rotina.custo_materializacao_do_dia as custo_rotina,
  execucoes_dbt_correcoes.custo_materializacao_do_dia as custo_dbt_correcoes,
  execucoes_dbt_livre_demanda.custo_materializacao_do_dia as custo_dbt_livre_demanda,
  execucoes_studio_livre_demanda.custo_materializacao_do_dia as custo_studio_livre_demanda
from execucoes_dbt_rotina
  full outer join execucoes_dbt_correcoes using(dia)
  full outer join execucoes_dbt_livre_demanda using(dia)
  full outer join execucoes_studio_livre_demanda using(dia)
order by 1 desc