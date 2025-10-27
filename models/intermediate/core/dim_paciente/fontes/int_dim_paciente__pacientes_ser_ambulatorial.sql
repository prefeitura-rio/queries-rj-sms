select distinct
    cast(null as string)                                     as paciente_cpf,
    cast(paciente_cns as string)                             as paciente_cns,
    cast(paciente_nome as string)                            as paciente_nome,
    cast(paciente_data_nascimento as date)                   as paciente_data_nascimento,
    cast(paciente_sexo as string)                            as paciente_sexo,
    cast(paciente_municipio as string)                       as paciente_municipio_residencia
from {{ ref ("raw_ser_metabase__ambulatorial") }}
where date(data_solicitacao) >= date '2024-01-01'