with pacientes as (
    select distinct
        -- id
        cast(null as string) as paciente_cpf,
        cast(paciente_cns as string) as paciente_cns,
        cast(paciente_nome as string) as paciente_nome,
        cast(paciente_data_nascimento as date) as paciente_data_nascimento,

        cast(mun.nome_municipio as string) as paciente_municipio_residencia
        
    from {{ ref("raw_ser_metabase__internacoes") }}
    left join {{ref("raw_sheets__municipios_rio")}} as mun
    on id_paciente_municipio_ibge = safe_cast(mun.cod_ibge_6 as int)
    where date(data_solicitacao) >= date '2024-01-01'
)

select * from pacientes