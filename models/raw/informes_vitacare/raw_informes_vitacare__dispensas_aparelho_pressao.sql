{{
    config(
        alias="dispensas_aparelho_pressao",
        materialized="table",
    )
}}

with
    source as (
        select 
          *
        from {{ source("brutos_informes_vitacare_staging", "dispensas_aparelho_pressao") }}
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
            {{ process_null('cnes') }} as unidade_cnes,
            {{ process_null('unidade_de_saude') }} as unidade_nome,
            {{ process_null('cns_paciente') }} as paciente_cns,
            {{ process_null('cpf_paciente') }} as paciente_cpf,
            {{ process_null('nome_paciente') }} as paciente_nome,
            {{ process_null('sexo') }} as sexo,
            cast({{ process_null('dta_nasc') }} as date format 'YYYY-MM-DD') as data_nascimento,
            {{ process_null('desig_medicamento_insumo') }} as desig_medicamento_insumo,
            {{ process_null('codigo') }} as codigo,
            {{ process_null('desig_lote') }} as desig_lote,
            cast({{ process_null('data_dispensacao') }} as timestamp) as data_dispensacao,
            {{ process_null('tipo_movimento') }} as tipo_movimento,
            {{ process_null('quantidade') }} as quantidade,
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