with pacientes as (
    select  
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

        concat(
            coalesce(paciente_tel_ddd, ''),
            coalesce(paciente_tel_num, '')
        ) as paciente_telefone,

        -- proxy de atualização cadastral: data de emissão do AIH mais recente.
        -- escolhemos data_emissao (e não data_internacao) porque representa o momento
        -- em que o paciente foi registrado no AIH, mais próximo de "cadastro atualizado".
        safe_cast(data_emissao as timestamp) as data_atualizacao

    from {{ ref("raw_sih__autorizacoes_internacoes_hospitalares") }}
    left join {{ref("raw_sheets__municipios_rio")}} as mun
    on safe_cast(paciente_municipio as int) = safe_cast(mun.cod_ibge_6 as int)
)

select * from pacientes
qualify row_number() over (
    partition by coalesce(safe_cast(paciente_cpf as string), safe_cast(paciente_cns as string))
    order by data_atualizacao desc
) = 1