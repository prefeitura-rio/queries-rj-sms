with
-- criando esquema canonico para unir todos os sistemas
    todos_registros as (
        select
    -- fonte dos dados
            'sisreg' as sistema_origem,

    -- id paciente
            paciente_cpf,
            paciente_cns,
            paciente_nome,
            paciente_nome_mae,
            --cast(null as string) as paciente_nome_pai,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

    -- sociodemograficos
            paciente_sexo,
            cast(null as string) as paciente_racacor,

    -- endereco
            /*
            paciente_uf_nascimento,
            paciente_municipio_nascimento,
            paciente_uf_residencia,
            paciente_municipio_residencia,
            paciente_bairro_residencia,
            paciente_cep_residencia,
            paciente_endereco_residencia,
            paciente_complemento_residencia,
            paciente_numero_residencia,
            paciente_tp_logradouro_residencia,
            */

            paciente_telefone,
            --cast(null as string) as paciente_email,
            
            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone      
        from {{ref("int_dim_paciente__pacientes_sisreg")}}

        union all

        select
            'siscan' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            paciente_nome_mae,
            --cast(null as string) as paciente_nome_pai,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            paciente_sexo,
            cast(null as string) as paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            cast(null as string) as paciente_municipio_nascimento,
            paciente_uf_residencia,
            paciente_municipio_residencia,
            paciente_bairro_residencia,
            paciente_cep_residencia,
            paciente_endereco_residencia,
            paciente_complemento_residencia,
            paciente_numero_residencia,
            cast(null as string) as paciente_tp_logradouro_residencia,
            */

            paciente_telefone,
            --cast(null as string) as paciente_email,
            
            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone    
        from {{ref("int_dim_paciente__pacientes_siscan")}}

        union all

        select
            'ser_internacoes' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            --cast(null as string) as paciente_nome_pai,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            cast(null as string) as paciente_sexo,
            cast(null as string) as paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            cast(null as string) as paciente_municipio_nascimento,
            cast(null as string) as paciente_uf_residencia,
            cast(null as string) as paciente_municipio_residencia,
            cast(null as string) as paciente_bairro_residencia,
            cast(null as string) as paciente_cep_residencia,
            cast(null as string) as paciente_endereco_residencia,
            cast(null as string) as paciente_complemento_residencia,
            cast(null as string) as paciente_numero_residencia,
            cast(null as string) as paciente_tp_logradouro_residencia,
            */

            cast(null as string) as paciente_telefone,
            --cast(null as string) as paciente_email,
            

            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone    
        from {{ref("int_dim_paciente__pacientes_ser_internacoes")}}

        union all

        select
            'ser_ambulatorial' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            --cast(null as string) as paciente_nome_pai,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            paciente_sexo,
            cast(null as string) as paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            cast(null as string) as paciente_municipio_nascimento,
            cast(null as string) as paciente_uf_residencia,
            paciente_municipio_residencia,
            cast(null as string) as paciente_bairro_residencia,
            cast(null as string) as paciente_cep_residencia,
            cast(null as string) as paciente_endereco_residencia,
            cast(null as string) as paciente_complemento_residencia,
            cast(null as string) as paciente_numero_residencia,
            cast(null as string) as paciente_tp_logradouro_residencia,
            */

            cast(null as string) as paciente_telefone,
            --cast(null as string) as paciente_email,
            
            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone    
        from {{ref("int_dim_paciente__pacientes_ser_ambulatorial")}}

        union all

        select
            'sih' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            --cast(null as string) as paciente_nome_pai,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            paciente_sexo,
            paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            paciente_municipio_nascimento,
            paciente_uf as paciente_uf_residencia,
            paciente_municipio as paciente_municipio_residencia,
            paciente_bairro as paciente_bairro_residencia,
            paciente_cep as paciente_cep_residencia,
            paciente_endereco_residencia,
            paciente_complemento as paciente_complemento_residencia,
            paciente_numero as paciente_numero_residencia,
            paciente_tp_logradouro_residencia,
            */

            paciente_telefone,
            --cast(null as string) as paciente_email,
            
            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone    
        from {{ref("int_dim_paciente__pacientes_sih")}}

        union all

        select
            'profissionais_cnes' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            --cast(null as string) as paciente_nome_pai,
            cast(null as date) as paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            cast(null as string) as paciente_sexo,
            cast(null as string) as paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            cast(null as string) as paciente_municipio_nascimento,
            cast(null as string) as paciente_uf_residencia,
            cast(null as string) as paciente_municipio_residencia,
            cast(null as string) as paciente_bairro_residencia,
            cast(null as string) as paciente_cep_residencia,
            cast(null as string) as paciente_endereco_residencia,
            cast(null as string) as paciente_complemento_residencia,
            cast(null as string) as paciente_numero_residencia,
            cast(null as string) as paciente_tp_logradouro_residencia,
            */

            cast(null as string) as paciente_telefone,
            --cast(null as string) as paciente_email,
            
            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone    
        from {{ref("int_dim_paciente__profissionais_cnes")}}

        union all

        select
            'minha_saude' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            --cast(null as string) as paciente_nome_pai,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            paciente_sexo,
            paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            cast(null as string) as paciente_municipio_nascimento,
            paciente_uf_residencia,
            paciente_municipio_residencia,
            paciente_bairro_residencia,
            cast(null as string) as paciente_cep_residencia,
            cast(null as string) as paciente_endereco_residencia,
            cast(null as string) as paciente_complemento_residencia,
            cast(null as string) as paciente_numero_residencia,
            cast(null as string) as paciente_tp_logradouro_residencia,
            */

            cast(null as string) as paciente_telefone,
            --cast(null as string) as paciente_email,
            
            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone    
        from {{ref("int_dim_paciente__pacientes_minha_saude")}}

        union all

        select
            'tea' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            --cast(null as string) as paciente_nome_pai,
            cast(null as date) as paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            cast(null as string) as paciente_sexo,
            cast(null as string) as paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            cast(null as string) as paciente_municipio_nascimento,
            cast(null as string) as paciente_uf_residencia,
            cast(null as string) as paciente_municipio_residencia,
            cast(null as string) as paciente_bairro_residencia,
            cast(null as string) as paciente_cep_residencia,
            cast(null as string) as paciente_endereco_residencia,
            cast(null as string) as paciente_complemento_residencia,
            cast(null as string) as paciente_numero_residencia,
            cast(null as string) as paciente_tp_logradouro_residencia,
            */

            cast(null as string) as paciente_telefone,
            --cast(null as string) as paciente_email,
            
            cast(null as int) as paciente_obito_ano,

            clinica_sf,
            clinica_sf_ap,
            clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone    
        from {{ref("int_dim_paciente__pacientes_tea")}}

        union all

        select
            'fibromialgia' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            --cast(null as string) as paciente_nome_pai,
            cast(null as date) as paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            cast(null as string) as paciente_sexo,
            cast(null as string) as paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            cast(null as string) as paciente_municipio_nascimento,
            cast(null as string) as paciente_uf_residencia,
            cast(null as string) as paciente_municipio_residencia,
            cast(null as string) as paciente_bairro_residencia,
            cast(null as string) as paciente_cep_residencia,
            cast(null as string) as paciente_endereco_residencia,
            cast(null as string) as paciente_complemento_residencia,
            cast(null as string) as paciente_numero_residencia,
            cast(null as string) as paciente_tp_logradouro_residencia,
            */

            cast(null as string) as paciente_telefone,
            --cast(null as string) as paciente_email,
            
            cast(null as int) as paciente_obito_ano,

            clinica_sf,
            clinica_sf_ap,
            clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone    
        from {{ref("int_dim_paciente__pacientes_fibromialgia")}}

        union all

        select
            'sipni' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            paciente_nome_mae,
            --paciente_nome_pai,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            cast(null as string) as paciente_sexo,
            cast(null as string) as paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            cast(null as string) as paciente_municipio_nascimento,
            paciente_uf_residencia,
            paciente_municipio_residencia,
            paciente_bairro_residencia,
            paciente_cep_residencia,
            cast(null as string) as paciente_endereco_residencia,
            cast(null as string) as paciente_complemento_residencia,
            cast(null as string) as paciente_numero_residencia,
            cast(null as string) as paciente_tp_logradouro_residencia,
            */

            cast(null as string) as paciente_telefone,
            --cast(null as string) as paciente_email,
            
            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone    
        from {{ref("int_dim_paciente__pacientes_sipni")}}

        union all

        select
            'hci' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            paciente_nome_mae,
            --paciente_nome_pai,
            paciente_data_nascimento,
            paciente_nome_social,

            paciente_sexo,
            paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            cast(null as string) as paciente_municipio_nascimento,
            cast(null as string) as paciente_uf_residencia,
            cast(null as string) as paciente_municipio_residencia,
            cast(null as string) as paciente_bairro_residencia,
            cast(null as string) as paciente_cep_residencia,
            cast(null as string) as paciente_endereco_residencia,
            cast(null as string) as paciente_complemento_residencia,
            cast(null as string) as paciente_numero_residencia,
            cast(null as string) as paciente_tp_logradouro_residencia,
            */

            cast(null as string) as paciente_telefone,
            --cast(null as string) as paciente_email,
            
            paciente_obito_ano,

            clinica_sf,
            clinica_sf_ap,
            clinica_sf_telefone,

            equipe_sf,
            equipe_sf_telefone
        from {{ref("int_dim_paciente__pacientes_hci")}}

        union all

        select
            'receita_federal' as sistema_origem,

            paciente_cpf,
            cast(null as int) as paciente_cns,
            paciente_nome,
            paciente_nome_mae,
            --cast(null as string) as paciente_nome_pai,
            paciente_data_nascimento,
            paciente_nome_social,

            paciente_sexo,
            cast(null as string) as paciente_racacor,

            /*
            cast(null as string) as paciente_uf_nascimento,
            cast(null as string) as paciente_municipio_nascimento,
            paciente_uf_residencia,
            paciente_municipio_residencia,
            paciente_bairro_residencia,
            paciente_cep_residencia,
            paciente_endereco_residencia,
            paciente_complemento_residencia,
            paciente_numero_residencia,
            paciente_tp_logradouro_residencia,
            */

            paciente_telefone,
            --paciente_email,
            
            paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone, -- DEU MERDA

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone    
        from {{ref("int_dim_paciente__pacientes_bcadastro")}}
    ),

-- backfill cpf
    todos_registros_enriquecido as (
        select
            ar.sistema_origem,

            lpad(
              safe_cast(
                    coalesce(
                        ar.paciente_cpf,
                        m.cpf
                    ) as string
                ),
                11, '0'
              ) as paciente_cpf, -- backfill

            safe_cast(coalesce(ar.paciente_cpf, m.cpf) as int) as cpf_particao,
            ar.paciente_cns,
            ar.paciente_nome,
            ar.paciente_nome_mae,
            --ar.paciente_nome_pai,
            ar.paciente_nome_social,

            ar.paciente_data_nascimento,
            ar.paciente_sexo,
            ar.paciente_racacor,

            /*
            ar.paciente_uf_nascimento,
            ar.paciente_municipio_nascimento,
            ar.paciente_uf_residencia,
            ar.paciente_municipio_residencia,
            ar.paciente_bairro_residencia,
            ar.paciente_cep_residencia,
            ar.paciente_endereco_residencia,
            ar.paciente_complemento_residencia,
            ar.paciente_numero_residencia,
            ar.paciente_tp_logradouro_residencia,
            */

            ar.paciente_telefone,
            --ar.paciente_email,
            
            ar.paciente_obito_ano,

            ar.clinica_sf,
            ar.clinica_sf_ap,
            ar.clinica_sf_telefone,

            ar.equipe_sf,
            ar.equipe_sf_telefone
        from todos_registros ar
            left join {{ref("int_dim_paciente__relacao_cns_cpf")}} m
            on safe_cast(ar.paciente_cns as int) = safe_cast(m.cns as int)
    )

select * from todos_registros_enriquecido
