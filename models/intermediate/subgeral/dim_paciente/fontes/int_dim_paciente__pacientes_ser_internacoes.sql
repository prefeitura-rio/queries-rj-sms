with pacientes as (
    select 
        -- id
        cast(null as int) as paciente_cpf,
        safe_cast(paciente_cns as int) as paciente_cns,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(paciente_data_nascimento as date) as paciente_data_nascimento,

        --safe_cast(mun.nome_municipio as string) as paciente_municipio_residencia
        
    from {{ ref("raw_ser_metabase__internacoes") }}
    left join {{ref("raw_sheets__municipios_rio")}} as mun
    on id_paciente_municipio_ibge = safe_cast(mun.cod_ibge_6 as int)
    where data_solicitacao >= "2024-01-01"
)

select distinct * from pacientes