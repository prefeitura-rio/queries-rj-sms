select distinct
    cast(null as string)                                     as paciente_cpf,  -- not present here
    cast(paciente_cns as string)                             as paciente_cns,
    cast(paciente_nome as string)                            as paciente_nome,
    cast(paciente_data_nasc as date)                         as paciente_data_nascimento,
    cast(paciente_sexo as string)                            as paciente_sexo,
    cast(paciente_nome_mae as string)                        as paciente_nome_mae,
    cast(null as string)                                     as paciente_nome_pai,
    cast(paciente_idade as int64)                            as paciente_idade,
    cast(paciente_uf as string)                              as paciente_uf_residencia,
    cast(paciente_municipio as string)                       as paciente_municipio_residencia,
    cast(paciente_bairro as string)                          as paciente_bairro_residencia,
    cast(paciente_cep as string)                             as paciente_cep_residencia,
    cast(paciente_logradouro as string)                      as paciente_endereco_residencia,
    cast(paciente_endereco_complemento as string)            as paciente_complemento_residencia,
    cast(paciente_endereco_numero as string)                 as paciente_numero_residencia
from {{ ref("raw_siscan_web__laudos") }}
where date(data_solicitacao) >= date '2024-01-01'