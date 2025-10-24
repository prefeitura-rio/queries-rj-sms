select distinct
    cast(paciente_cpf as string)                             as paciente_cpf,
    cast(paciente_cns as string)                             as paciente_cns,
    cast(paciente_nome as string)                            as paciente_nome,
    cast(paciente_data_nascimento as date)                   as paciente_data_nascimento,
    cast(paciente_sexo as string)                            as paciente_sexo,
    cast(paciente_nome_mae as string)                        as paciente_nome_mae,
    cast(paciente_uf_nascimento as string)                   as paciente_uf_nascimento,
    cast(paciente_municipio_nascimento as string)            as paciente_municipio_nascimento,
    cast(paciente_uf_residencia as string)                   as paciente_uf_residencia,
    cast(paciente_municipio_residencia as string)            as paciente_municipio_residencia,
    cast(paciente_bairro_residencia as string)               as paciente_bairro_residencia,
    cast(paciente_cep_residencia as string)                  as paciente_cep_residencia,
    cast(paciente_endereco_residencia as string)             as paciente_endereco_residencia,
    cast(paciente_complemento_residencia as string)          as paciente_complemento_residencia,
    cast(paciente_numero_residencia as string)               as paciente_numero_residencia,
    cast(paciente_tp_logradouro_residencia as string)        as paciente_tp_logradouro_residencia
  
from {{ ref("mart_sisreg__solicitacoes") }}
where date(data_solicitacao) >= date '2025-01-01'