with pacientes as (
    select distinct 
        -- id
        cast(paciente_cpf as string) as paciente_cpf,
        cast(paciente_cns as string) as paciente_cns,
        cast(paciente_nome as string) as paciente_nome, 
        cast(paciente_data_nascimento as date) as paciente_data_nascimento,

        case 
            when paciente_sexo = 'M' then 'MASCULINO'
            when paciente_sexo = 'F' then 'FEMININO'
            else NULL
        end as paciente_sexo,

        case 
            when safe_cast(paciente_raca_cor as int) = 1 then "BRANCA"
            when safe_cast(paciente_raca_cor as int) = 2 then "PRETA"
            when safe_cast(paciente_raca_cor as int) = 3 then "PARDA"
            when safe_cast(paciente_raca_cor as int) = 4 then "AMARELA"
            when safe_cast(paciente_raca_cor as int) = 5 then "INDIGENA"
            else NULL 
        end as paciente_racacor,

        cast(paciente_mun_origem as string) as paciente_municipio_nascimento,
        cast(paciente_complemento as string) as paciente_complemento,
        cast(paciente_numero as string) as paciente_numero,
        cast(paciente_logradouro as string) as paciente_endereco_residencia,
        cast(paciente_cep as string) as paciente_cep,
        cast(paciente_tipo_logradouro as string) as paciente_tp_logradouro_residencia,
        cast(paciente_bairro as string) as paciente_bairro,
        cast(mun.nome_municipio as string) as paciente_municipio,
        cast(paciente_uf as string) as paciente_uf
        
    from {{ ref("raw_sih__autorizacoes_internacoes_hospitalares") }}
    left join {{ref("raw_sheets__municipios_rio")}} as mun
    on safe_cast(paciente_municipio as int) = safe_cast(mun.cod_ibge_6 as int)
)

select * from pacientes
-- paciente_sexo: I, -, M, F
-- paciente_racacor: 0,1,2,3,4,5,99