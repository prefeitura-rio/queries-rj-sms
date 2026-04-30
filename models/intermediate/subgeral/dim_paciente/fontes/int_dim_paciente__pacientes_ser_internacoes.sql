with pacientes as (
    select 
        -- id
        cast(null as int) as paciente_cpf,
        safe_cast(paciente_cns as int) as paciente_cns,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(paciente_data_nascimento as date) as paciente_data_nascimento,

        -- proxy de atualização cadastral: data da solicitação de internação mais recente no SER.
        safe_cast(data_solicitacao as timestamp) as data_atualizacao

    from {{ ref("raw_ser_metabase__internacoes") }}
    left join {{ref("raw_sheets__municipios_rio")}} as mun
    on id_paciente_municipio_ibge = safe_cast(mun.cod_ibge_6 as int)
    where data_solicitacao >= "2024-01-01"
)

select * from pacientes
-- SER internações não tem cpf; particiona por cns.
qualify row_number() over (
    partition by paciente_cns
    order by data_atualizacao desc
) = 1