with pacientes as (
        select
        -- id
            safe_cast(cpf as int) as paciente_cpf,
            cns as paciente_cns_array,
            upper(dados.nome) as paciente_nome,
            dados.data_nascimento as paciente_data_nascimento,
            upper(dados.mae_nome) as paciente_nome_mae,

            upper(dados.nome_social) as paciente_nome_social,
            upper(dados.genero) as paciente_sexo,
            upper(dados.raca) as paciente_racacor,

            extract(year from dados.obito_data) as paciente_obito_ano,

            estabs.nome_acentuado as clinica_sf,
            estabs.area_programatica as clinica_sf_ap,
            estabs.telefone[SAFE_OFFSET(0)] as clinica_sf_telefone,

            upper(equipe_saude_familia [SAFE_OFFSET(0)].nome) as equipe_sf,
            equipe_saude_familia [SAFE_OFFSET(0)].telefone as equipe_sf_telefone

        from {{ ref("mart_historico_clinico__paciente") }}
        left join {{ ref("dim_estabelecimento") }} as estabs
        on safe_cast(
            equipe_saude_familia[SAFE_OFFSET(0)].clinica_familia.id_cnes as int
            ) = safe_cast(id_cnes as int)
  
    ),

    pacientes_cns_unnested as (
        select
            p.paciente_cpf,
            safe_cast(cns_item as int) as paciente_cns,
            p.paciente_nome,
            p.paciente_data_nascimento,
            p.paciente_nome_mae,
            p.paciente_nome_social,
            p.paciente_sexo,
            p.paciente_racacor,
            paciente_obito_ano,
            p.clinica_sf_ap,
            p.clinica_sf,
            p.equipe_sf,
            p.equipe_sf_telefone,
            p.clinica_sf_telefone,

            -- HCI é um snapshot consolidado: cada paciente aparece uma única vez, já deduplicado
            -- upstream. Não há "data de atualização por paciente" observável aqui.
            -- A prioridade do HCI é garantida pela ordem fixa de sistemas, não por data.
            cast(null as timestamp) as data_atualizacao
        from pacientes p
            left join unnest (p.paciente_cns_array) as cns_item
    )

select * from pacientes_cns_unnested
qualify row_number() over (
    partition by coalesce(safe_cast(paciente_cpf as string), safe_cast(paciente_cns as string))
    order by paciente_cns nulls last
) = 1