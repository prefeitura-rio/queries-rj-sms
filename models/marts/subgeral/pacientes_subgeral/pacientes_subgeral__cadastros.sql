-- noqa: disable=LT08
{{
  config(
    enabled=true,
    schema="pacientes_subgeral",
    alias="cadastros_pacientes",
    unique_key='cpf',
    partition_by={
        "field": "cpf_particao",
        "data_type": "int64",
        "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
    on_schema_change='sync_all_columns',
    tags=["weekly"]
  )
}}

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
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

    -- sociodemograficos
            paciente_sexo,
            cast(null as string) as paciente_racacor,

            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone,
            data_atualizacao  
        from {{ref("int_dim_paciente__pacientes_sisreg")}}

        union all

        select
            'siscan' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            paciente_nome_mae,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            paciente_sexo,
            cast(null as string) as paciente_racacor,

            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone,
            data_atualizacao
        from {{ref("int_dim_paciente__pacientes_siscan")}}

        union all

        select
            'ser_internacoes' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            cast(null as string) as paciente_sexo,
            cast(null as string) as paciente_racacor,

            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone,
            data_atualizacao
        from {{ref("int_dim_paciente__pacientes_ser_internacoes")}}

        union all

        select
            'ser_ambulatorial' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            paciente_sexo,
            cast(null as string) as paciente_racacor,

            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone,
            data_atualizacao
        from {{ref("int_dim_paciente__pacientes_ser_ambulatorial")}}

        union all

        select
            'sih' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            paciente_sexo,
            paciente_racacor,

            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone,
            data_atualizacao
        from {{ref("int_dim_paciente__pacientes_sih")}}

        union all

        select
            'minha_saude' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            paciente_sexo,
            paciente_racacor,

            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone,
            data_atualizacao
        from {{ref("int_dim_paciente__pacientes_minha_saude")}}

        union all

        select
            'tea' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            cast(null as date) as paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            cast(null as string) as paciente_sexo,
            cast(null as string) as paciente_racacor,

            cast(null as int) as paciente_obito_ano,

            clinica_sf,
            clinica_sf_ap,
            clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone,
            data_atualizacao
        from {{ref("int_dim_paciente__pacientes_tea")}}

        union all

        select
            'fibromialgia' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            cast(null as string) as paciente_nome_mae,
            cast(null as date) as paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            cast(null as string) as paciente_sexo,
            cast(null as string) as paciente_racacor,

            cast(null as int) as paciente_obito_ano,

            clinica_sf,
            clinica_sf_ap,
            clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone,
            data_atualizacao
        from {{ref("int_dim_paciente__pacientes_fibromialgia")}}

        union all

        select
            'sipni' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            paciente_nome_mae,
            paciente_data_nascimento,
            cast(null as string) as paciente_nome_social,

            cast(null as string) as paciente_sexo,
            cast(null as string) as paciente_racacor,

            cast(null as int) as paciente_obito_ano,

            cast(null as string) as clinica_sf,
            cast(null as string) as clinica_sf_ap,
            cast(null as string) as clinica_sf_telefone,

            cast(null as string) as equipe_sf,
            cast(null as string) as equipe_sf_telefone,
            data_atualizacao
        from {{ref("int_dim_paciente__pacientes_sipni")}}

        union all

        select
            'hci' as sistema_origem,

            paciente_cpf,
            paciente_cns,
            paciente_nome,
            paciente_nome_mae,
            paciente_data_nascimento,
            paciente_nome_social,

            paciente_sexo,
            paciente_racacor,

            paciente_obito_ano,

            clinica_sf,
            clinica_sf_ap,
            clinica_sf_telefone,

            equipe_sf,
            equipe_sf_telefone,
            data_atualizacao
        from {{ref("int_dim_paciente__pacientes_hci")}}
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
            ar.paciente_nome_social,

            ar.paciente_data_nascimento,
            
            case
                when ar.paciente_sexo not in (
                    "FEMININO", "MASCULINO"
                ) then null
                else ar.paciente_sexo
            end as paciente_sexo,
            
            case 
                when ar.paciente_racacor not in (
                    "BRANCA", "PRETA", "PARDA", "AMARELA", "INDIGENA"
                ) then null
                else ar.paciente_racacor
            end as paciente_racacor, 
            
            ar.paciente_obito_ano,

            ar.clinica_sf,
            ar.clinica_sf_ap,
            ar.clinica_sf_telefone,

            ar.equipe_sf,
            ar.equipe_sf_telefone,

            ar.data_atualizacao
        from todos_registros ar
            left join {{ref("pacientes_subgeral__relacao_cns_cpf")}} m
            on safe_cast(ar.paciente_cns as int) = safe_cast(m.cns as int)
    )

select * from todos_registros_enriquecido
