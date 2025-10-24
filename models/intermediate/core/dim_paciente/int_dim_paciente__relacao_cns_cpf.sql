with
pares_cpf_cns as (
    select paciente_cpf, paciente_cns from {{ref("int_dim_paciente__pacientes_sisreg")}} where paciente_cpf is not null and paciente_cns is not null
    union all select paciente_cpf, paciente_cns from {{ref("int_dim_paciente__profissionais_cnes")}} where paciente_cpf is not null and paciente_cns is not null
    union all select paciente_cpf, paciente_cns from {{ref("int_dim_paciente__pacientes_sih")}}  where paciente_cpf is not null and paciente_cns is not null
    union all select paciente_cpf, paciente_cns from {{ref("int_dim_paciente__pacientes_minha_saude")}} where paciente_cpf is not null and paciente_cns is not null
    union all select paciente_cpf, paciente_cns from {{ref("int_dim_paciente__pacientes_tea")}} where paciente_cpf is not null and paciente_cns is not null
    union all select paciente_cpf, paciente_cns from {{ref("int_dim_paciente__pacientes_fibromialgia")}} where paciente_cpf is not null and paciente_cns is not null
    union all select paciente_cpf, paciente_cns from {{ref("int_dim_paciente__pacientes_sipni")}} where paciente_cpf is not null and paciente_cns is not null
),

cns_para_cpf_unico as (
  select
    paciente_cns as cns,
    any_value(paciente_cpf) as cpf
  from pares_cpf_cns
  group by paciente_cns
  having count(distinct paciente_cpf) = 1
)

select * from cns_para_cpf_unico
