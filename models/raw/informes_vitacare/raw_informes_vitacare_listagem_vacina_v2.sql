{{
    config(
        alias="listagem_vacina_v2",
        materialized="table",
    )
}}

with
    source as (
        select 
            *
        from {{ source("brutos_informes_vitacare_staging", "listagem_vacina_v2") }}
    ),
    
    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by _source_file, indice order by _loaded_at desc) = 1 
    ),

    extrair_informacoes as (
        select
            {{ process_null('ap') }} as ap,
            {{ process_null('n_cnes_unidade') }} as cnes_unidade,
            {{ process_null('nome_unidade_de_saude') }} as unidade_nome,
            {{ process_null('nome_equipe_de_saude') }} as nome_equipe,
            {{ process_null('codigo_da_equipe_de_saude') }} as codigo_equipe_saude,
            {{ process_null('codigo_ine_equipe_de_saude') }} as codigo_ine_equipe_saude,
            {{ process_null('codigo_microarea') }} as codigo_microarea,
            {{ process_null('n_do_prontuario') }} as n_prontuario,
            {{ process_null('n_cpf') }} as cpf,
            {{ validate_cpf('n_cpf') }} as cpf_valido,
            {{ process_null('n_cns_da_pessoa_cadastrada') }} as n_cns_pessoa_cadastrada,
            {{ process_null('nome_da_pessoa_cadastrada') }} as nome_pessoa_cadastrada,
            case 
                when REGEXP_CONTAINS({{ process_null('data_de_nascimento') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_de_nascimento as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_de_nascimento') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(data_de_nascimento as date format 'DD/MM/YYYY')
                else null
            end as data_nascimento, 
            {{ process_null('nome_da_mae_pessoa_cadastrada') }} as nome_da_mae_pessoa_cadastrada,
            case 
                when REGEXP_CONTAINS({{ process_null('data_de_nasc_mae') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_de_nasc_mae as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_de_nasc_mae') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(data_de_nasc_mae as date format 'DD/MM/YYYY')
                else null
            end as data_nasc_mae, 
            {{ process_null('situacao_usuario') }} as situacao_usuario,
            {{ process_null('obito') }} as obito,
            {{ process_null('data_cadastro') }} as data_cadastro,
            {{ process_null('tipo_de_logradouro') }} as tipo_logradouro,
            {{ process_null('logradouro') }} as logradouro,
            {{ process_null('numero_logradouro') }} as numero_logradouro,
            {{ process_null('complemento_logradouro') }} as complemento_logradouro,
            {{ process_null('cep_logradouro') }} as cep_logradouro,
            {{ process_null('bairro_logradouro') }} as bairro_logradouro,
            {{ process_null('data_da_ultima_consulta_enf') }} as data_ultima_consuilta_enf,
            case 
                when REGEXP_CONTAINS({{ process_null('data_da_ultima_visita_acs') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_da_ultima_visita_acs as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_da_ultima_visita_acs') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(data_da_ultima_visita_acs as date format 'DD/MM/YYYY')
                else null
            end as data_ultima_visita_acs, 
            {{ process_null('calendario_vacinal_atualizado') }} as calendario_vacinal_atualizado,
            {{ process_null('vacina') }} as vacina,
            case
                when REGEXP_CONTAINS({{ process_null('data_aplicacao') }}, r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}')
                    then cast({{ process_null('data_aplicacao') }} as timestamp format 'DD/MM/YYYY HH24:MI')
                when REGEXP_CONTAINS({{ process_null('data_aplicacao') }}, r'\d{2}/\d{2}/\d{4}')
                    then cast(concat({{ process_null('data_aplicacao') }}, ' 00:00') as timestamp format 'DD/MM/YYYY HH24:MI')
                else null
            end as data_aplicacao,
            case
                when REGEXP_CONTAINS({{ process_null('data_registro') }}, r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}')
                    then cast({{ process_null('data_registro') }} as timestamp format 'DD/MM/YYYY HH24:MI')
                when REGEXP_CONTAINS({{ process_null('data_registro') }}, r'\d{2}/\d{2}/\d{4}')
                    then cast(concat({{ process_null('data_registro') }}, ' 00:00') as timestamp format 'DD/MM/YYYY HH24:MI')
                else null
            end as data_registro,
            {{ process_null('diff') }} as diff,
            {{ process_null('dose_vtc') }} as dose_vtc,
            {{ process_null('tipo_registro') }} as tipo_registro,
            {{ process_null('estrategia_imunizacao') }} as estrategia_imunizacao,
            {{ process_null('cbo') }} as cbo,
            {{ process_null('profissional') }} as profissional,
            {{ process_null('performedvaccineactionid') }} as performedvaccineactionid,
            {{ process_null('ano_particao') }} as ano_particao,
            {{ process_null('mes_particao') }} as mes_particao,
            {{ process_null('data_particao') }} as data_particao,
            struct(
                _source_file as arquivo_fonte,
                safe_cast(REGEXP_EXTRACT(_source_file, r'/(\d{4}-\d{2}-\d{2})/') as timestamp) as criado_em,
                SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', _extracted_at) as extraido_em,
                SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', _loaded_at) as carregado_em
            ) as metadados
        from sem_duplicatas
    )
select * from extrair_informacoes