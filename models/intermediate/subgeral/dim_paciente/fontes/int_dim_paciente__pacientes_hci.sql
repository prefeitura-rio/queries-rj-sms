with pacientes as (
        select distinct
        -- id
            safe_cast(cpf as int) as paciente_cpf,
            cns as paciente_cns_array,
            upper(dados.nome) as paciente_nome,
            dados.data_nascimento as paciente_data_nascimento,
            upper(dados.mae_nome) as paciente_nome_mae,

            upper(dados.nome_social) as paciente_nome_social,
            upper(dados.genero) as paciente_sexo,
            upper(dados.raca) as paciente_racacor,
            upper(dados.pai_nome) as paciente_nome_pai,

            extract(year from dados.obito_data) as paciente_obito_ano,

            estabs.nome_acentuado as clinica_sf,
            estabs.area_programatica as clinica_sf_ap,
            estabs.telefone[SAFE_OFFSET(0)] as clinica_sf_telefone,

            upper(equipe_saude_familia [SAFE_OFFSET(0)].nome) as equipe_sf,
            equipe_saude_familia [SAFE_OFFSET(0)].telefone as equipe_sf_telefone

            /* to-do arrays: 
            endereco.complemento as paciente_complemento_residencia,
            ebdereco.numero as paciente_numero_residencia,
            endereco.cep as paciente_cep_residencia,
            endereco.logradouro as paciente_endereco_residencia,
            enereco.tipo_logradouro as paciente_tp_logradouro_residencia,
            endereco.bairro as paciente_bairro_residencia,
            endereco.cidade as paciente_municipio_residencia,
            endereco.estado as paciente_uf_residencia,

            contato.telefone.ddd,
            contato.telefone.valor,
            contato.email.valor,       

            equipe_saude_familia.id_ine,
            equipe_saude_familia.clinica_familia.id_cnes,
            */

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
            p.paciente_nome_pai,
            paciente_obito_ano,
            p.clinica_sf_ap,
            p.clinica_sf,
            p.equipe_sf,
            p.equipe_sf_telefone,
            p.clinica_sf_telefone
        from pacientes p
            left join unnest (p.paciente_cns_array) as cns_item
    )

select * from pacientes_cns_unnested
-- sexo: feminino, masculino
-- racacor: parda, preta, branca, amarela