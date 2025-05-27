{{
    config(
        alias="acompanhamento_mulheres_idade_fertil",
        materialized="table",
    )
}}

with
    source as (
        select 
          *
        from {{ source("brutos_informes_vitacare_staging", "acompanhamento_mulheres_idade_fertil") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by _source_file, indice order by _loaded_at desc) = 1 
    ),

    extrair_informacoes as (
        select
            REGEXP_EXTRACT(_source_file, r'^(AP\d+)') AS ap,
            REGEXP_EXTRACT(_source_file, r'^AP\d+/(\d{4}-\d{2})') AS mes_referencia,
            {{ process_null('cnes_da_unidade') }} as unidade_cnes,
            {{ process_null('nome_da_unidade') }} as unidade_nome,
            {{ process_null('ine_da_equipe') }} as equipe_ine,
            {{ process_null('cns_do_paciente') }} as paciente_cns,
            {{ process_null('cpf_do_paciente') }} as paciente_cpf,
            {{ process_null('n_prontuario_do_paciente') }} as numero_prontuario,
            {{ process_null('nome_do_paciente') }} as nome_paciente,
            case 
                when REGEXP_CONTAINS({{ process_null('data_de_nascimento_da_paciente') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_de_nascimento_da_paciente as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_de_nascimento_da_paciente') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(data_de_nascimento_da_paciente as date format 'DD/MM/YYYY')
                else null
            end as data_nascimento,            
            {{ process_null('idade_da_paciente') }} as idade_paciente,
            {{ process_null('raca_cor') }} as raca_cor,
            {{ process_null('metodo_escolha') }} as metodo_escolha,
            {{ process_null('implanon___data_inicio') }} as implanon_data_inicio,
            {{ process_null('sigtap___03_01_04_017_6') }} as sigtap_0301040176,
            {{ process_null('sigtap___03_01_04_018_4') }} as sigtap_0301040184,
            case 
                when REGEXP_CONTAINS({{ process_null('data_da_insercao') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_da_insercao as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_da_insercao') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(data_da_insercao as date format 'DD/MM/YYYY')
                else null
            end as data_insercao,
            case 
                when REGEXP_CONTAINS({{ process_null('diu___data_de_inicio') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(diu___data_de_inicio as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('diu___data_de_inicio') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(diu___data_de_inicio as date format 'DD/MM/YYYY')
                else null
            end as diu_data_de_inicio,
            case 
                when REGEXP_CONTAINS({{ process_null('diu___data_termino') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(diu___data_termino as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('diu___data_termino') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(diu___data_termino as date format 'DD/MM/YYYY')
                else null
            end as diu_data_termino,                
            {{ process_null('cid_ativo_hipertensao') }} as cid_ativo_hipertensao,
            {{ process_null('cid_ativo_diabetes') }} as cid_ativo_diabetes,
            {{ process_null('cid_ativo_obesidade_sobrepeso') }} as cid_ativo_obesidade_sobrepeso,
            case 
                when REGEXP_CONTAINS({{ process_null('data_da_mamografia') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_da_mamografia as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_da_mamografia') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(data_da_mamografia as date format 'DD/MM/YYYY')
                else null
            end as data_mamografia,  
            {{ process_null('citologia___sim___nao') }} as citologia_indicador,
            case 
                when REGEXP_CONTAINS({{ process_null('citologia____data_do_resultado') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(citologia____data_do_resultado as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('citologia____data_do_resultado') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(citologia____data_do_resultado as date format 'DD/MM/YYYY')
                else null
            end as citologia_data_resultado,  
            {{ process_null('teste_rapido___procedimento') }} as teste_rapido_procedimento,
            {{ process_null('teste_rapido___procedimento___profissional') }} as teste_rapido_procedimento_profissional,
            case 
                when REGEXP_CONTAINS({{ process_null('teste_rapido___procedimento___data_de_registro') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(teste_rapido___procedimento___data_de_registro as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('teste_rapido___procedimento___data_de_registro') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(teste_rapido___procedimento___data_de_registro as date format 'DD/MM/YYYY')
                else null
            end as teste_rapido_procedimento_data_de_registro, 
            case 
                when REGEXP_CONTAINS({{ process_null('resultado___data_de_registro') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(resultado___data_de_registro as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('resultado___data_de_registro') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(resultado___data_de_registro as date format 'DD/MM/YYYY')
                else null
            end as resultado_data_registros, 
            {{ process_null('cetonuria') }} as cetonuria,
            {{ process_null('media_das_glicemias_capilares') }} as media_glicemias_capilares,
            {{ process_null('glicemia_capilar__jejum') }} as glicemia_capilar_jejum,
            {{ process_null('glicemia_capilar_apos_as_refeicoes') }} as glicemia_capilar_apos_refeicoes,
            {{ process_null('glicose_na_urina') }} as glicose_urina,
            {{ process_null('teste_de_gravidez') }} as teste_gravidez,
            {{ process_null('teste_de_gravidez___observacoes') }} as teste_gravidez_observacoes,
            {{ process_null('teste_rapido_hepatite_b') }} as teste_rapido_hepatite_b,
            {{ process_null('teste_rapido_hepatite_b___observacoes') }} as teste_rapido_hepatite_b_observacoes,
            {{ process_null('teste_rapido_hepatite_c') }} as teste_rapido_hepatite_c,
            {{ process_null('teste_rapido_hepatite_c___observacoes') }} as teste_rapido_hepatite_c_observacoes,
            {{ process_null('teste_rapido_hiv') }} as teste_rapido_hiv,
            {{ process_null('teste_rapido_hiv___observacoes') }} as teste_rapido_hiv_observacoes,
            {{ process_null('ppd') }} as ppd,
            case 
                when REGEXP_CONTAINS({{ process_null('data_ppd') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_ppd as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_ppd') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(data_ppd as date format 'DD/MM/YYYY')
                else null
            end as data_ppd, 
            {{ process_null('teste_rapido_sifilis') }} as teste_rapido_sifilis,
            {{ process_null('teste_rapido_sifilis___observacoes') }} as teste_rapido_sifilis_observacoes,
            {{ process_null('teste_rapido_tuberculose') }} as teste_rapido_tuberculose,
            {{ process_null('cbo_do_profissional') }} as cbo_profissional,
            {{ process_null('cns_do_profissional') }} as cns_profissional,
            {{ process_null('nome_do_profissional') }} as nome_profissional,
            struct(
                _source_file as arquivo_fonte,
                safe_cast(REGEXP_EXTRACT(_source_file, r'/(\d{4}-\d{2}-\d{2})/') as timestamp) as criado_em,
                safe_cast(_extracted_at as timestamp) as extraido_em,
                safe_cast(_loaded_at as timestamp) as carregado_em
            ) as metadados
        from sem_duplicatas
    )
select 
* 
from extrair_informacoes