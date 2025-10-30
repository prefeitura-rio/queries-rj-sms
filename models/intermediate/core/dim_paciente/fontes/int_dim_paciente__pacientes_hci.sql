with pacientes as (
    select distinct 
        -- id
        cpf as paciente_cpf,
        cns as paciente_cns_array,
        upper(dados.nome) as paciente_nome,
        dados.data_nascimento as paciente_data_nascimento,
        upper(dados.mae_nome) as paciente_nome_mae,

        upper(dados.nome_social) as paciente_nome_social,
        upper(dados.genero) as paciente_sexo,
        upper(dados.raca) as paciente_racacor,
        upper(dados.pai_nome) as paciente_nome_pai,

        /* to-do arrays: 
        endereco.complemento as paciente_complemento_residencia,
        ebdereco.numero as paciente_numero_residencia,
        endereco.cep as paciente_cep_residencia,
        endereco.logradouro as paciente_endereco_residencia,
        enereco.tipo_logradouro as paciente_tp_logradouro_residencia,
        endereco.bairro as paciente_bairro_residencia,
        endereco.cidade as paciente_municipio_residencia,
        endereco.estado as paciente_uf_residencia
        */

    from {{ ref("mart_historico_clinico__paciente") }}
),

pacientes_cns_unnested as (
    select
        p.paciente_cpf,
        cns_item as paciente_cns,
        p.paciente_nome, 
        p.paciente_data_nascimento,
        p.paciente_nome_mae, 
        p.paciente_nome_social,
        p.paciente_sexo, 
        p.paciente_racacor, 
        p.paciente_nome_pai
    from pacientes p
    left join unnest(p.paciente_cns_array) as cns_item
)

select * from pacientes_cns_unnested
-- sexo: feminino, masculino
-- racacor: parda, preta, branca, amarela