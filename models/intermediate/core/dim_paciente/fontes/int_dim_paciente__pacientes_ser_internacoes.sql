select distinct
    cast(null as string)                                     as paciente_cpf,
    cast(paciente_cns as string)                             as paciente_cns,
    cast(paciente_nome as string)                            as paciente_nome,
    cast(paciente_data_nascimento as date)                   as paciente_data_nascimento,
    cast(id_paciente_municipio_ibge as string)               as id_paciente_municipio_ibge
from {{ ref("raw_ser_metabase__internacoes") }}
where date(data_solicitacao) >= date '2025-01-01'