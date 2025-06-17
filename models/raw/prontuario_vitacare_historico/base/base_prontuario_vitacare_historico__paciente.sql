{{
    config(
        schema="brutos_prontuario_vitacare_historico_staging",
        alias="_base_paciente_historico",
        materialized="table",
    )
}}


with

    source as (
        select * 
        from {{ source("brutos_prontuario_vitacare_historico_staging", "pacientes") }}
    ),

    remove_unwanted_characters as (

        select

            concat(
                nullif({{ remove_double_quotes("id_cnes") }}, ''),
                '.',
                nullif({{ clean_numeric_string(remove_double_quotes("ut_id")) }}, '')
            ) as id,

            -- Outras Chaves
            {{ remove_double_quotes("id_cnes") }} as id_cnes,
            {{ clean_numeric_string(remove_double_quotes("ut_id")) }} as id_local,
            {{ remove_double_quotes("npront") }} as numero_prontuario,
            {{ remove_double_quotes("cpf") }} as cpf,
            {{ remove_double_quotes("dnv") }} as dnv,
            {{ remove_double_quotes("nis") }} as nis,
            {{ remove_double_quotes("cns") }} as cns,

            -- Informações Pessoais
            {{ remove_double_quotes("nome") }} as nome,
            {{ remove_double_quotes("nomesocial") }} as nome_social,
            {{ remove_double_quotes("nomemae") }} as nome_mae,
            {{ remove_double_quotes("nomepai") }} as nome_pai,
            {{ remove_double_quotes("obito") }} as obito,
            {{ remove_double_quotes("sexo") }} as sexo,
            {{ remove_double_quotes("orientacaosexual") }} as orientacao_sexual,
            {{ remove_double_quotes("identidadegenero") }} as identidade_genero,
            {{ remove_double_quotes("racacor") }} as raca_cor,

            -- Informações Cadastrais
            {{ remove_double_quotes("situacaousuario") }} as situacao,
            {{ remove_double_quotes("cadastropermanente") }} as cadastro_permanente,

            if(
                not regexp_contains(
                    {{ remove_double_quotes("datacadastro") }},
                    r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$'
                ),
                null,
                {{ remove_double_quotes("datacadastro") }}
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
            {{ remove_double_quotes("nacionalidade") }} as nacionalidade,

            if(
                not regexp_contains(
                    {{ remove_double_quotes("dta_nasc") }}, r'^\d{4}-\d{2}-\d{2}$'
                ),
                null,
                {{ remove_double_quotes("dta_nasc") }}
            ) as data_nascimento,

            {{ remove_double_quotes("paisnascimento") }} as pais_nascimento,
            {{ remove_double_quotes("municipionascimento") }} as municipio_nascimento,
            {{ remove_double_quotes("estadonascimento") }} as estado_nascimento,

            -- Contato
            {{ remove_double_quotes("email") }} as email,
            {{ remove_double_quotes("telefone") }} as telefone,

            -- Endereço
            {{ remove_double_quotes("tipodomicilio") }} as endereco_tipo_domicilio,
            {{ remove_double_quotes("tipologradouro") }} as endereco_tipo_logradouro,
            {{ remove_double_quotes("cep") }} as endereco_cep,
            {{ remove_double_quotes("logradouro") }} as endereco_logradouro,
            {{ remove_double_quotes("bairro") }} as endereco_bairro,
            {{ remove_double_quotes("estadoresidencia") }} as endereco_estado,
            {{ remove_double_quotes("municipioresidencia") }} as endereco_municipio,

            -- Informações da Unidade
            {{ remove_double_quotes("ap") }} as ap,
            {{ remove_double_quotes("microarea") }} as microarea,
            cast(null as string) as nome_unidade,
            {{ remove_double_quotes("codigoequipe") }} as codigo_equipe_saude,
            {{ remove_double_quotes("ineequipe") }} as codigo_ine_equipe_saude,

            if(
                not regexp_contains(
                    dataatualizacaovinculoequipe,
                    r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$'
                ),
                null,
                dataatualizacaovinculoequipe
            ) as data_atualizacao_vinculo_equipe,

            -- Metadata columns
            {{ remove_double_quotes("extracted_at") }} as loaded_at

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
                when loaded_at = '' or loaded_at is null
                then null
                else safe_cast(loaded_at as datetime)
            end as loaded_at

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
            data_particao,
            source_created_at,
            source_updated_at,
            loaded_at,

        from deduplicated
    )

select *
from final