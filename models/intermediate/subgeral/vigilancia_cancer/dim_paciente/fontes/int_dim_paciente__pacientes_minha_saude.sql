with pacientes as (
    select distinct
        -- id
        cast(mongo.cpf as string) as paciente_cpf,
        cast(mongo.cns as string) as paciente_cns,
        cast(mongo.nome as string) as paciente_nome,
        cast(ms.datanascimento as date) as paciente_data_nascimento,
        
        case 
            when ms.sexo = 'F' THEN 'FEMININO'
            when ms.sexo = 'M' THEN 'MASCULINO'
        else NULL
        end as paciente_sexo,

        case 
            when ms.racacor = "None" then NULL
            when ms.racacor = "SEM INFORMACAO" then NULL
            else ms.racacor
        end as paciente_racacor,

        cast(ms.bairroresidencia as string) as paciente_bairro_residencia,
        cast(ms.municipioresidencia as string) as paciente_municipio_residencia,
        cast(ms.ufresidencia as string) as paciente_uf_residencia

    from {{ ref("raw_centralderegulacao_mysql__minha_saude__lista_usuario") }} as ms
    left join (
        select distinct
            cast(idusuario as string) as idusuario,
            cast(cpf as string) as cpf,
            cast(cns as string) as cns,
            cast(nome as string) as nome
        from {{ ref("raw_minhasaude_mongodb__perfil_acessos") }}
    ) as mongo
    on cast(ms.idusuario as string) = mongo.idusuario
    )

select * from pacientes
-- paciente_racacor: AMARELA, BRANCA, INDIGENA, "None", PARDA, PRETA, SEM INFORMACAO
-- paciente_sexo: F, M