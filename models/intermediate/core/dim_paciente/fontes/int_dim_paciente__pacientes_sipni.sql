select distinct 
    cast(nu_cpf_paciente as string)                          as paciente_cpf,
    cast(nu_cns_paciente as string)                          as paciente_cns,
    cast(no_paciente as string)                              as paciente_nome, 
    cast(dt_nascimento_paciente as date)                     as paciente_data_nascimento,
    cast(tp_sexo_paciente as string)                         as paciente_sexo,
    cast(no_mae_paciente as string)                          as paciente_nome_mae,
    cast(no_pai_paciente as string)                          as paciente_nome_pai,
    cast(no_bairro_paciente as string)                       as paciente_bairro_residencia,
    cast(nu_cep_paciente as string)                          as paciente_cep_residencia,
    cast(no_municipio_paciente as string)                    as paciente_municipio_residencia,
    cast(no_uf_paciente as string)                           as paciente_uf_residencia,
    cast(no_pais_paciente as string)                         as paciente_pais_residencia
from {{ ref("raw_sipni__vacinacao") }}
where date(dt_vacina) >= date '2024-01-01'