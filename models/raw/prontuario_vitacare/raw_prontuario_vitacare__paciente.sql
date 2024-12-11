{{
    config(
        alias="paciente",
        materialized="table",
    )
}}

-- dbt run --select raw_prontuario_vitacare__paciente
with
    paciente as (
        select *, 'rotineiro' as tipo,
        from {{ ref("base_prontuario_vitacare__paciente_rotineiro") }}
        union all
        select *, 'historico' as tipo,
        from {{ ref("base_prontuario_vitacare__paciente_historico") }}
    ),

    paciente_deduplicado as (
        select *,
        from paciente
        qualify
            row_number() over (
                partition by id order by source_updated_at desc
            ) = 1
    ),

    corrige_cadastro as (
        select

            * except (cadastro_permanente, nome_social, sexo, raca_cor, nome_mae, obito),

            case when nome_social in ('') then null else nome_social end as nome_social,

            sexo,

            case
                when trim(raca_cor) in ("", "NAO INFORMADO", "SEM INFORMACAO")
                then null
                else initcap(raca_cor)
            end as raca_cor,

            case
                when obito = '1' or obito = 'True'
                then true
                else false
            end as obito_indicador,

            case when nome_mae in ("NONE") then null else nome_mae end as nome_mae,

            case
                when cadastro_permanente = "True" or codigo_ine_equipe_saude is not null
                then true
                when cadastro_permanente = "False"
                then false
                else false
            end as cadastro_permanente_indicador,

            case
                when codigo_ine_equipe_saude is not null then true else false
            end as equipe_familia_indicador,

        from paciente_deduplicado
    ),

    renomeado as (
        select
            -- PK
            {{ remove_accents_upper("id") }} as id,


            -- Outras Chaves
            {{ remove_accents_upper("id_cnes") }} as id_cnes,
            numero_prontuario,
            {{ remove_accents_upper("cpf") }} as cpf,
            {{ remove_accents_upper("cns") }} as cns,

            -- Informações Pessoais
            {{ remove_accents_upper("nome") }} as nome,
            {{ remove_accents_upper("nome_social") }} as nome_social,
            {{ remove_accents_upper("nome_mae") }} as mae_nome,
            {{ remove_accents_upper("nome_pai") }} as pai_nome,
            obito_indicador,
            {{ remove_accents_upper("sexo") }} as sexo,
            date(data_nascimento) as data_nascimento,
            {{ remove_accents_upper("orientacao_sexual") }} as orientacao_sexual,
            {{ remove_accents_upper("identidade_genero") }} as identidade_genero,
            {{ remove_accents_upper("raca_cor") }} as raca,

            -- Informações Cadastrais
            situacao,
            cadastro_permanente_indicador,
            {{ validate_cpf(remove_accents_upper("cpf")) }} as cpf_valido_indicador,
            data_cadastro_inicial,
            data_ultima_atualizacao_cadastral,

            -- Contato
            {{ remove_accents_upper("telefone") }} as telefone,
            {{ remove_accents_upper("email") }} as email,

            -- Endereço
            {{ padronize_cep(remove_accents_upper("endereco_cep")) }} as endereco_cep,
            {{ remove_accents_upper("endereco_tipo_logradouro") }} as endereco_tipo_logradouro,
            {{
                remove_accents_upper(
                    'REGEXP_EXTRACT(endereco_logradouro, r"^(.*?)(?:\d+.*)?$")'
                )
            }} as endereco_logradouro,
            {{
                remove_accents_upper(
                    'REGEXP_EXTRACT(endereco_logradouro, r"\b(\d+)\b")'
                )
            }} as endereco_numero,
            {{
                remove_accents_upper(
                    'REGEXP_REPLACE(endereco_logradouro, r"^.*?\d+\s*(.*)$", r"\1")'
                )
            }} as endereco_complemento,
            {{ remove_accents_upper("endereco_bairro") }} as endereco_bairro,
            {{ remove_accents_upper("endereco_municipio") }} as endereco_municipio,
            {{ remove_accents_upper("endereco_estado") }} as endereco_estado,

            -- Informações da Unidade
            equipe_familia_indicador,
            {{ remove_accents_upper("codigo_ine_equipe_saude") }} as id_ine,
            data_atualizacao_vinculo_equipe,


            -- Metadados
            source_created_at,
            source_updated_at,

        from corrige_cadastro
    ),

    pacientes_validos as (
        select * from renomeado where cpf is not null and id_cnes is not null
    )

select *
from pacientes_validos
