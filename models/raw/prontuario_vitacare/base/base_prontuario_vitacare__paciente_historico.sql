{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_paciente_historico",
        materialized="incremental",
        incremental_strategy='merge',
        unique_key="id_paciente_global",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=['monthly']
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with

    source_cadastro as (
        select
            *
        from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
        {% if is_incremental() %}
        where loaded_at > '{{ seven_days_ago }}'
        {% endif %}
    ),

    selecao_pacientes as (
        select
            -- PK
            id_global as id_paciente_global,

            -- Chave identificadora para pacientes sem CPF
            -- cnes + ut_id = id_paciente_global
            -- ut_id = id_paciente_local
            id_local as id_paciente_local,

            -- Outras Chaves
            id_cnes,
            npront as numero_prontuario,
            cpf,
            dnv,
            nis,
            cns,

            -- Informações Pessoais
            nome,
            nome_social,
            nome_mae,
            nome_pai,
            obito,
            sexo,
            orientacao_sexual,
            identidade_genero,
            raca_cor,

            -- Informações Cadastrais
            situacao_usuario as situacao,
            cadastro_permanente,
            safe_cast(data_cadastro as timestamp) as data_cadastro_inicial,
            safe_cast(data_atualizacao_cadastro as timestamp) as data_ultima_atualizacao_cadastral,

            -- Nascimento
            nacionalidade,
            data_nascimento,
            pais_nascimento,
            municipio_nascimento,
            estado_nascimento,

            -- Contato
            email,
            telefone,

            -- Endereço
            tipo_domicilio as endereco_tipo_domicilio,
            tipo_logradouro as endereco_tipo_logradouro,
            cep as endereco_cep,
            logradouro as endereco_logradouro,
            bairro as endereco_bairro,
            estado_residencia as endereco_estado,
            municipio_residencia as endereco_municipio,

            -- Informações da Unidade
            ap,
            microarea,
            unidade as nome_unidade,
            codigo_equipe as codigo_equipe_saude,
            ine_equipe as codigo_ine_equipe_saude,
            safe_cast(data_atualizacao_vinculo_equipe as timestamp) as data_atualizacao_vinculo_equipe,

            date(safe_cast(loaded_at as datetime)) as data_particao,
            safe_cast(data_cadastro as timestamp) as source_created_at,
            safe_cast(data_atualizacao_cadastro as timestamp) as source_updated_at,
            safe_cast(loaded_at as timestamp) as datalake_imported_at,
            greatest(
                safe_cast(data_atualizacao_cadastro as timestamp),
                safe_cast(data_cadastro as timestamp),
                safe_cast(data_atualizacao_vinculo_equipe as timestamp)
            ) as updated_at_rank

        from source_cadastro

        qualify row_number() over (
            partition by id_paciente_global
            order by updated_at_rank desc
        ) = 1
    )

select
    *
from selecao_pacientes