with pacientes as (
    select
        -- id
        safe_cast(mongo.cpf as int) as paciente_cpf,
        safe_cast(mongo.cns as int) as paciente_cns,
        safe_cast(mongo.nome as string) as paciente_nome,
        safe_cast(ms.datanascimento as date) as paciente_data_nascimento,

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

        coalesce(
            safe_cast(ms.ultimaatualizacaocadsus as timestamp),
            safe_cast(mongo.updatedat as timestamp),
            safe_cast(ms.datahoracadastro as timestamp)
        ) as data_atualizacao

    from {{ ref("raw_centralderegulacao_mysql__minha_saude__lista_usuario") }} as ms
    left join (
        select
            cast(idusuario as string) as idusuario,
            cast(cpf as string) as cpf,
            cast(cns as string) as cns,
            cast(nome as string) as nome,
            updatedat
        from {{ ref("raw_minhasaude_mongodb__perfil_acessos") }}
    ) as mongo
    on cast(ms.idusuario as string) = mongo.idusuario
    )

select * from pacientes
qualify row_number() over (
    partition by coalesce(safe_cast(paciente_cpf as string), safe_cast(paciente_cns as string))
    order by data_atualizacao desc nulls last
) = 1