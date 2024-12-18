{{
    config(
        alias="_paciente_historico",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


with

    source as (
        select * except (backup_created_at),  -- TODO: change data type to string to correct load the column in bigquery
        from {{ source("brutos_prontuario_vitacare_staging", "pacientes_historico") }}
    ),

    remove_unwanted_characters as (

        select

            -- PK
            concat(
                nullif(id_cnes, ''),
                '.',
                nullif({{ clean_numeric_string("ut_id") }}, '')
            ) as id,

            -- Outras Chaves
            {{ remove_accents_upper("id_cnes") }} as id_cnes,
            {{ clean_numeric_string("ut_id") }} as id_local,
            {{ remove_accents_upper("npront") }} as numero_prontuario,
            {{ remove_accents_upper("cpf") }} as cpf,
            {{ remove_accents_upper("dnv") }} as dnv,
            {{ remove_accents_upper("nis") }} as nis,
            {{ remove_accents_upper("cns") }} as cns,

            -- Informações Pessoais
            {{ remove_accents_upper("nome") }} as nome,
            {{ remove_accents_upper("nomesocial") }} as nome_social,
            {{ remove_accents_upper("nomemae") }} as nome_mae,
            {{ remove_accents_upper("nomepai") }} as nome_pai,
            {{ remove_accents_upper("obito") }} as obito,
            {{ remove_accents_upper("sexo") }} as sexo,
            {{ remove_accents_upper("orientacaosexual") }} as orientacao_sexual,
            {{ remove_accents_upper("identidadegenero") }} as identidade_genero,
            {{ remove_accents_upper("racacor") }} as raca_cor,

            -- Informações Cadastrais
            {{ remove_accents_upper("situacaousuario") }} as situacao,
            {{ remove_accents_upper("cadastropermanente") }} as cadastro_permanente,

            if(
                not regexp_contains(
                    {{ remove_accents_upper("datacadastro") }},
                    r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$'
                ),
                null,
                {{ remove_accents_upper("datacadastro") }}
            ) as data_cadastro_inicial,

            if(
                not regexp_contains(
                    dataatualizacaocadastro,
                    r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$'
                ),
                null,
                dataatualizacaocadastro
            ) as data_ultima_atualizacao_cadastral,

            -- Nascimento
            {{ remove_accents_upper("nacionalidade") }} as nacionalidade,

            if(
                not regexp_contains(
                    {{ remove_accents_upper("dta_nasc") }}, r'^\d{4}-\d{2}-\d{2}$'
                ),
                null,
                {{ remove_accents_upper("dta_nasc") }}
            ) as data_nascimento,

            {{ remove_accents_upper("paisnascimento") }} as pais_nascimento,
            {{ remove_accents_upper("municipionascimento") }} as municipio_nascimento,
            {{ remove_accents_upper("estadonascimento") }} as estado_nascimento,

            -- Contato
            {{ remove_accents_upper("email") }} as email,
            {{ remove_accents_upper("telefone") }} as telefone,

            -- Endereço
            {{ remove_accents_upper("tipodomicilio") }} as endereco_tipo_domicilio,
            {{ remove_accents_upper("tipologradouro") }} as endereco_tipo_logradouro,
            {{ remove_accents_upper("cep") }} as endereco_cep,
            {{ remove_accents_upper("logradouro") }} as endereco_logradouro,
            {{ remove_accents_upper("bairro") }} as endereco_bairro,
            {{ remove_accents_upper("estadoresidencia") }} as endereco_estado,
            {{ remove_accents_upper("municipioresidencia") }} as endereco_municipio,

            -- Informações da Unidade
            {{ remove_accents_upper("ap") }} as ap,
            {{ remove_accents_upper("microarea") }} as microarea,
            cast(null as string) as nome_unidade,
            {{ remove_accents_upper("codigoequipe") }} as codigo_equipe_saude,
            {{ remove_accents_upper("ineequipe") }} as codigo_ine_equipe_saude,

            if(
                not regexp_contains(
                    dataatualizacaovinculoequipe,
                    r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$'
                ),
                null,
                dataatualizacaovinculoequipe
            ) as data_atualizacao_vinculo_equipe,

            -- Metadata columns
            {{ remove_accents_upper("datalake_imported_at") }} as datalake_imported_at

        from source
    ),

    cast_to_correct_types as (
        select
            * except (
                data_nascimento,
                data_atualizacao_vinculo_equipe,
                data_cadastro_inicial,
                data_ultima_atualizacao_cadastral
            ),

            safe_cast(data_nascimento as date format 'YYYY-MM-DD') as data_nascimento,

            parse_timestamp(
                '%Y-%m-%d %H:%M:%E3S', data_atualizacao_vinculo_equipe
            ) as data_atualizacao_vinculo_equipe,

            parse_timestamp(
                '%Y-%m-%d %H:%M:%E3S', data_cadastro_inicial
            ) as data_cadastro_inicial,

            parse_timestamp(
                '%Y-%m-%d %H:%M:%E3S', data_ultima_atualizacao_cadastral
            ) as data_ultima_atualizacao_cadastral

        from remove_unwanted_characters
    ),

    standardized as (

        select

            -- PK
            id,

            -- OUTRAS CHAVES
            id_cnes,

            case
                when
                    cpf = ''
                    or regexp_contains(cpf, r'^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$')
                then null
                else cpf
            end as cpf,

            case
                when
                    dnv = ''
                    or regexp_contains(dnv, r'^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$')
                then null
                else dnv
            end as dnv,

            case
                when
                    nis = ''
                    or regexp_contains(nis, r'^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$')
                then null
                else nis
            end as nis,

            case
                when
                    cns = ''
                    or regexp_contains(cns, r'^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$')
                then null
                else cns
            end as cns,

            id_local,


            -- INFORMAÇÕES PESSOAIS
            case {{ remove_invalid_names("nome") }} else nome end as nome,

            case
                {{ remove_invalid_names("nome_social") }} else nome_social
            end as nome_social,

            case {{ remove_invalid_names("nome_mae") }} else nome_mae end as nome_mae,

            case {{ remove_invalid_names("nome_pai") }} else nome_pai end as nome_pai,

            case when obito = '1' then 'True' else 'False' end as obito,

            case when sexo = '' then null else lower(sexo) end as sexo,

            case
                when orientacao_sexual = '' then null else orientacao_sexual
            end as orientacao_sexual,

            case
                when
                    identidade_genero in (
                        "CIS",
                        "HETEROSSEXUAL",
                        "BISSEXUAL",
                        "HOMOSSEXUAL (GAY / LESBICA)"
                    )
                then lower("CIS")
                when identidade_genero in ("MULHER TRANSEXUAL")
                then lower("MULHER TRANSEXUAL")
                when identidade_genero in ("HOMEM TRANSEXUAL")
                then lower("HOMEM TRANSEXUAL")
                when identidade_genero in ("TRAVESTI", "OUTRO")
                then lower("OUTRO")
                else null
            end as identidade_genero,

            case
                when raca_cor in ("AMARELA", "BRANCA", "INDÍGENA", "PARDA", "PRETA")
                then cast(lower(raca_cor) as string)
                else null
            end as raca_cor,

            -- CONTATO
            case {{ remove_invalid_email("email") }} else email end as email,

            case
                when
                    telefone = ''
                    or regexp_contains(telefone, r'^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$')
                    or regexp_contains(telefone, r'^\b21\b$')
                then null
                else telefone
            end as telefone,

            -- NASCIMENTO
            case
                when nacionalidade = '' then null else lower(nacionalidade)
            end as nacionalidade,

            case
                when data_nascimento > current_date() then null else data_nascimento
            end as data_nascimento,

            case
                when pais_nascimento = '' then null else lower(pais_nascimento)
            end as pais_nascimento,

            case
                when municipio_nascimento = ''
                then null
                else lower(municipio_nascimento)
            end as municipio_nascimento,  -- No rotineiro é o codigo IBGE, aqui esta sendo o nome

            case
                when estado_nascimento = '' then null else lower(estado_nascimento)
            end as estado_nascimento,

            -- INFORMAÇÕES DA UNIDADE
            case when ap = '' then null else ap end as ap,

            case when microarea = '' then null else microarea end as microarea,

            case when nome_unidade = '' then null else nome_unidade end as nome_unidade,

            case
                when codigo_equipe_saude = '' then null else codigo_equipe_saude
            end as codigo_equipe_saude,

            case
                when codigo_ine_equipe_saude = '' then null else codigo_ine_equipe_saude
            end as codigo_ine_equipe_saude,

            case
                when
                    cast(data_atualizacao_vinculo_equipe as date) > current_date()
                    or cast(data_atualizacao_vinculo_equipe as date) = '1900-01-01'
                then null
                else data_atualizacao_vinculo_equipe
            end as data_atualizacao_vinculo_equipe,

            case
                when numero_prontuario = '' then null else numero_prontuario
            end as numero_prontuario,

            case
                when cadastro_permanente = '' then null else cadastro_permanente
            end as cadastro_permanente,

            case
                when situacao = '' then null else situacao
            end as situacao,

            case
                when
                    cast(data_cadastro_inicial as date) > current_date()
                    or cast(data_cadastro_inicial as date) = '1900-01-01'
                then null
                else data_cadastro_inicial
            end as data_cadastro_inicial,

            case
                when
                    cast(data_ultima_atualizacao_cadastral as date) > current_date()
                    or cast(data_ultima_atualizacao_cadastral as date) = '1900-01-01'
                then null
                else data_ultima_atualizacao_cadastral
            end as data_ultima_atualizacao_cadastral,

            -- ENDEREÇO
            case
                when endereco_tipo_domicilio = ''
                then null
                else lower(endereco_tipo_domicilio)
            end as endereco_tipo_domicilio,

            case
                when endereco_tipo_logradouro = ''
                then null
                else lower(endereco_tipo_logradouro)
            end as endereco_tipo_logradouro,

            case
                when endereco_cep = '' then null else lower(endereco_cep)
            end as endereco_cep,

            case
                when endereco_logradouro = '' then null else lower(endereco_logradouro)
            end as endereco_logradouro,

            case
                when endereco_bairro = '' then null else lower(endereco_bairro)
            end as endereco_bairro,

            case
                when endereco_estado = '' then null else lower(endereco_estado)
            end as endereco_estado,

            case
                when endereco_municipio = '' then null else lower(endereco_municipio)
            end as endereco_municipio,

            -- METADATA COLUMNS
            safe_cast(null as date) as data_particao,

            case
                when
                    cast(data_cadastro_inicial as date) > current_date()
                    or cast(data_cadastro_inicial as date) = '1900-01-01'
                then null
                else data_cadastro_inicial
            end as source_created_at,

            case
                when
                    cast(data_ultima_atualizacao_cadastral as date) > current_date()
                    or cast(data_ultima_atualizacao_cadastral as date) = '1900-01-01'
                then null
                else data_ultima_atualizacao_cadastral
            end as source_updated_at,

            case
                when datalake_imported_at = ''
                then null
                else parse_timestamp('%Y-%m-%d %H:%M:%E3S', datalake_imported_at)
            end as datalake_imported_at

        from cast_to_correct_types
    ),

    -- Cerca de 1000 registros estão duplicados, porém não há como identificar o
    -- registro mais recente.
    -- Ambos os registros compartilham as mesma data de atualização, apesar de haver
    -- diferença em pelos 1 campo
    -- Portanto, optou-se por manter apenas 1 registro de forma aleatória utilizando a
    -- função count(*) over (partition by gid)
    deduplicated as (
        select * from standardized qualify count(*) over (partition by id) = 1
    ),

    final as (

        select

            -- PK
            id,

            -- Outras Chaves
            id_cnes,
            id_local,
            numero_prontuario,
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
            situacao,
            cadastro_permanente,
            data_cadastro_inicial,
            data_ultima_atualizacao_cadastral,

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
            endereco_tipo_domicilio,
            endereco_tipo_logradouro,
            endereco_cep,
            endereco_logradouro,
            endereco_bairro,
            endereco_estado,
            endereco_municipio,

            -- Informações da Unidade
            ap,
            microarea,
            nome_unidade,
            codigo_equipe_saude,
            codigo_ine_equipe_saude,
            data_atualizacao_vinculo_equipe,

            -- Metadata columns
            source_created_at as created_at,
            source_updated_at as updated_at,
            datalake_imported_at as loaded_at,

            -- Particionamento
            safe_cast(safe_cast(datalake_imported_at as timestamp) as date) as data_particao,

        from deduplicated
    )

select *
from final
