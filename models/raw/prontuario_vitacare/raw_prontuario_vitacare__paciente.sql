{{
    config(
        schema = "brutos_prontuario_vitacare",
        alias="paciente",
        materialized="table",
        cluster_by=["id_paciente_global", "cpf", "cns"],
        tags=['daily']
    )
}}

-- dbt run --select raw_prontuario_vitacare__paciente
with
    paciente as (
        select *, 'historico' as tipo,
        from {{ ref("base_prontuario_vitacare__paciente_historico") }}
        union all 
        select *, 'continuo' as tipo,
        from {{ ref("base_prontuario_vitacare__paciente_continuo") }}
    ),

    paciente_deduplicado as (
        select *,
        from paciente
        qualify
            row_number() over (
                partition by id_paciente_global
                order by updated_at_rank desc
            ) = 1
    ),

    corrige_cadastro as (
        select

            * except (cadastro_permanente, nome_social, sexo, raca_cor, nome_mae, obito, codigo_ine_equipe_saude),
            {{process_null('codigo_ine_equipe_saude')}} as codigo_ine_equipe_saude,

            case when nome_social in ('') then null else nome_social end as nome_social,

            sexo,

            case
                when trim(raca_cor) in ("", "NAO INFORMADO", "SEM INFORMACAO")
                then null
                else initcap(raca_cor)
            end as raca_cor,

            obito as obito_indicador,

            case when nome_mae in ("NONE") then null else nome_mae end as nome_mae,

            case
                when cadastro_permanente is true 
                then true
                when cadastro_permanente is false
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
            id_paciente_global,

            -- Identificadores do paciente no VitaCare
            id_paciente_local,

            -- Outras Chaves
            id_cnes,
            numero_prontuario,
            {{ clean_numeric("cpf") }} as cpf,
            cns,

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
            {{ validate_cpf(clean_numeric("cpf")) }} as cpf_valido_indicador,
            timestamp_add(datetime(timestamp(data_cadastro_inicial), 'America/Sao_Paulo'), interval 3 hour) as data_cadastro_inicial,
            timestamp_add(datetime(timestamp(data_ultima_atualizacao_cadastral), 'America/Sao_Paulo'), interval 3 hour) as data_ultima_atualizacao_cadastral,

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
                    'REGEXP_EXTRACT(endereco_logradouro, r\"\\b(\\d+|S\\/?[Nn])\\b\")'
                )
            }} as endereco_numero,
            {{
                remove_accents_upper(
                    'REGEXP_EXTRACT(endereco_logradouro, r\"(?:\\b\\d+|\\bS\\/?[Nn])\\b\\s*[\\-|\\s]*(.*)$\" )'
                )
            }} as endereco_complemento,
            {{ remove_accents_upper("endereco_bairro") }} as endereco_bairro,
            {{ remove_accents_upper("endereco_municipio") }} as endereco_municipio,
            {{ remove_accents_upper("endereco_estado") }} as endereco_estado,

            -- Informações da Unidade
            equipe_familia_indicador,
            codigo_ine_equipe_saude as id_ine,
            data_atualizacao_vinculo_equipe,

            -- Metadados
            timestamp_add(datetime(timestamp(source_created_at), 'America/Sao_Paulo'), interval 3 hour) as source_created_at,
            timestamp_add(datetime(timestamp(source_updated_at), 'America/Sao_Paulo'), interval 3 hour) as source_updated_at,
            updated_at_rank

        from corrige_cadastro
    )

select *
from renomeado
