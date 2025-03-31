{{
    config(
        alias="acompanhamento_mensal_gestantes",
        materialized="table",
    )
}}

with
    source as (
        select 
          *
        from {{ source("brutos_informes_vitacare_staging", "acompanhamento_mensal_gestantes") }}
    ),
    extrair_informacoes as (
        select
            REGEXP_EXTRACT(_source_file, r'^(AP\d+)') AS ap,
            REGEXP_EXTRACT(_source_file, r'^AP\d+/(\d{4}-\d{2})') AS mes_referencia,
            {{ process_null('nome') }} as nome,
            {{ process_null('no_cartao') }} as no_cartao,
            {{ process_null('codigo_entidade') }} as codigo_entidade,
            {{ process_null('entidade') }} as entidade,
            {{ process_null('area') }} as area,
            {{ process_null('microarea') }} as microarea,
            {{ process_null('numero_do_prontuario') }} as numero_prontuario,
            cast({{ process_null('data_de_nascimento') }} as date format 'DD/MM/YYYY') as data_de_nascimento,
            {{ process_null('nome_da_mae') }} as nome_mae,
            {{ process_null('dnv') }} as dnv,
            {{ process_null('cpf') }} as cpf,
            {{ validate_cpf('cpf') }} as cpf_valido,
            cast({{ process_null('data_de_ativacao_do_diagnostico') }} as date format 'DD/MM/YYYY') as data_ativacao_diagnostico,
            cast({{ process_null('data_da_ultima_menstruacao') }} as date format 'DD/MM/YYYY') as data_ultima_menstruacao,
            cast({{ process_null('data_da_1a_cons_de_pre_natal_apos_a_dum') }} as date format 'DD/MM/YYYY') as data_primeira_consulta_pre_natal_apos_dum,
            {{ process_null('dpp') }} as dpp,
            {{ process_null('dpp_corrigido_por_ecografia') }} as dpp_corrigido_por_ecografia,
            {{ process_null('semana_gestacional_na_1a_cons_de_pre_natal') }} as semana_gestacional_na_1a_cons_de_pre_natal,
            {{ process_null('num_de_consultas_de_pre_natal') }} as num_de_consultas_de_pre_natal,
            cast({{ process_null('data_da_ult_cons_pre_natal') }} as date format 'DD/MM/YYYY') as data_da_ult_cons_pre_natal,
            {{ process_null('semana_gestacional_atual') }} as semana_gestacional_atual,
            {{ process_null('risco_gestacional') }} as risco_gestacional,
            {{ process_null('num_vds_do_acs_ate_38_semanas_de_gest') }} as num_vds_do_acs_ate_38_semanas_de_gest,
            cast({{ process_null('data_da_ult_vd_do_acs') }} as date format 'DD/MM/YYYY') as data_da_ult_vd_do_acs,
            {{ process_null('semana_gestacional_ult_vd_do_acs') }} as semana_gestacional_ult_vd_do_acs,
            cast({{ process_null('data_de_resolucao_do_diagnostico') }} as date format 'DD/MM/YYYY') as data_resolucao_do_diagnostico,
            case 
                when REGEXP_CONTAINS({{ process_null('data_do_parto') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_do_parto as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_do_parto') }},r'\d{2}\/\d{2}\/\d{4}') 
                    then cast(data_do_parto as date format 'DD/MM/YYYY')
                else null
            end as data_parto,
            {{ process_null('data_do_parto') }} as data_parto_raw,
            cast({{ process_null('data_de_realizacao_teste_rapido_para_sifilis') }} as date format 'DD/MM/YYYY') as data_realizacao_teste_rapido_sifilis,
            {{ process_null('resultado_teste_rapido_para_sifilis') }} as resultado_teste_rapido_sifilis,
            cast({{ process_null('data_de_realizacao_vdrl') }} as date format 'DD/MM/YYYY') as data_realizacao_vdrl,
            {{ process_null('resultado_vdrl') }} as resultado_vdrl,
            {{ process_null('parceiro_tratado') }} as parceiro_tratado,
            cast({{ process_null('data_de_realizacao_teste_rapido_para_hiv') }} as date format 'DD/MM/YYYY') as data_realizacao_teste_rapido_hiv,
            {{ process_null('resultado_teste_rapido_de_hiv') }} as resultado_teste_rapido_hiv,
            cast({{ process_null('data_da_coleta_hiv') }} as date format 'DD/MM/YYYY') as data_coleta_hiv,
            {{ process_null('resultado_de_hiv') }} as resultado_de_hiv,
            cast({{ process_null('data_de_realizacao_teste_rapido_gravidez') }} as date format 'DD/MM/YYYY') as data_realizacao_teste_rapido_gravidez,
            {{ process_null('resultado_teste_rapido_gravidez') }} as resultado_teste_rapido_gravidez,
            cast({{ process_null('data_de_consulta_de_sb') }} as date format 'DD/MM/YYYY') as data_consulta_sb,
            cast({{ process_null('data_de_procedimento_coletivo_sb') }} as date format 'DD/MM/YYYY') as data_procedimento_coletivo_sb,
            REGEXP_EXTRACT({{ process_null('periodo_de_extracao') }},r'([0-9\/]+) -') as periodo_extracao_inicio,
            REGEXP_EXTRACT({{ process_null('periodo_de_extracao') }},r'- ([0-9\/]+)') as periodo_extracao_fim,
            {{ process_null('periodo_de_extracao') }} as periodo_extracao,
            struct(
                _source_file as arquivo_fonte,
                cast(REGEXP_EXTRACT(_source_file, r'/(\d{4}-\d{2}-\d{2})/') as timestamp) as criado_em,
                cast(_extracted_at as timestamp) as extraido_em,
                cast(_loaded_at as timestamp) as carregado_em
            ) as metadados
        from source
    )
select 
* 
from extrair_informacoes