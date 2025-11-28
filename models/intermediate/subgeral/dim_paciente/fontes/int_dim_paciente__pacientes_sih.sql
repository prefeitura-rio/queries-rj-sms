with pacientes as (
    select distinct 
        -- id
        safe_cast(paciente_cpf as int) as paciente_cpf,
        safe_cast(paciente_cns as int) as paciente_cns,
        safe_cast(paciente_nome as string) as paciente_nome, 
        safe_cast(paciente_data_nascimento as date) as paciente_data_nascimento,

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

        /*
        safe_cast(paciente_mun_origem as string) as paciente_municipio_nascimento,
        safe_cast(paciente_complemento as string) as paciente_complemento,
        safe_cast(paciente_numero as string) as paciente_numero,
        safe_cast(paciente_logradouro as string) as paciente_endereco_residencia,
        safe_cast(paciente_cep as string) as paciente_cep,
        safe_cast(paciente_tipo_logradouro as string) as paciente_tp_logradouro_residencia,
        safe_cast(paciente_bairro as string) as paciente_bairro,
        safe_cast(mun.nome_municipio as string) as paciente_municipio,
        safe_cast(paciente_uf as string) as paciente_uf,
        */

        concat(
            coalesce(paciente_tel_ddd, ''),
            coalesce(paciente_tel_num, '')
        ) as paciente_telefone
        
    from {{ ref("raw_sih__autorizacoes_internacoes_hospitalares") }}
    left join {{ref("raw_sheets__municipios_rio")}} as mun
    on safe_cast(paciente_municipio as int) = safe_cast(mun.cod_ibge_6 as int)
)

select * from pacientes
-- paciente_sexo: I, -, M, F
-- paciente_racacor: 0,1,2,3,4,5,99