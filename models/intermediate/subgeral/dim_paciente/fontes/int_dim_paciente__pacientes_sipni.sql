with pacientes as (
    select distinct 
        -- id
        safe_cast(nu_cpf_paciente as int) as paciente_cpf,
        safe_cast(nu_cns_paciente as int) as paciente_cns,
        safe_cast(no_paciente as string) as paciente_nome, 
        safe_cast(dt_nascimento_paciente as date) as paciente_data_nascimento,
        safe_cast(no_mae_paciente as string) as paciente_nome_mae,
        safe_cast(no_pai_paciente as string) as paciente_nome_pai,
        
        case
            when tp_sexo_paciente = 'M' then 'MASCULINO'
            when tp_sexo_paciente = 'F' then 'FEMININO'
            else NULL
        end as paciente_sexo,

        safe_cast(nu_cep_paciente as string) as paciente_cep_residencia,
        safe_cast(no_bairro_paciente as string) as paciente_bairro_residencia,
        safe_cast(no_municipio_paciente as string) as paciente_municipio_residencia, 
        case 
            when no_uf_paciente = 'ACRE' then 'AC'
            when no_uf_paciente = 'ALAGOAS' then 'AL'
            when no_uf_paciente = 'AMAZONAS' then 'AM'
            when no_uf_paciente = 'AMAPA' then 'AP'
            when no_uf_paciente = 'BAHIA' then 'BA'
            when no_uf_paciente = 'CEARA' then 'CE'
            when no_uf_paciente = 'DISTRITO FEDERAL' then 'DF'
            when no_uf_paciente = 'ESPIRITO SANTO' then 'ES'
            when no_uf_paciente = 'GOIAS' then 'GO'
            when no_uf_paciente = 'MARANHAO' then 'MA'
            when no_uf_paciente = 'MATO GROSSO' then 'MT'
            when no_uf_paciente = 'MATO GROSSO DO SUL' then 'MS'
            when no_uf_paciente = 'MINAS GERAIS' then 'MG'
            when no_uf_paciente = 'PARA' then 'PA'
            when no_uf_paciente = 'PARAIBA' then 'PB'
            when no_uf_paciente = 'PARANA' then 'PR'
            when no_uf_paciente = 'PERNAMBUCO' then 'PE'
            when no_uf_paciente = 'PIAUI' then 'PI'
            when no_uf_paciente = 'RIO DE JANEIRO' then 'RJ'
            when no_uf_paciente = 'RIO GRANDE DO NORTE' then 'RN'
            when no_uf_paciente = 'RIO GRANDE DO SUL' then 'RS'
            when no_uf_paciente = 'RONDONIA' then 'RO'
            when no_uf_paciente = 'RORAIMA' then 'RR'
            when no_uf_paciente = 'SANTA CATARINA' then 'SC'
            when no_uf_paciente = 'SERGIPE' then 'SE'
            when no_uf_paciente = 'SAO PAULO' then 'SP'
            when no_uf_paciente = 'TOCANTINS' then 'TO'
            else NULL
        end as paciente_uf_residencia,
        safe_cast(no_pais_paciente as string) as paciente_pais_residencia
        
    from {{ ref("raw_sipni__vacinacao") }}
    where date(dt_vacina) >= date '2024-01-01'
)

select * from pacientes
-- paciente_sexo: M, F, I
-- ufs: RIO DE JANEIRO, SAO PAULO, MINAS GERAIS, AMAPA ..