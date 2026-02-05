{{
    config(
        schema="brutos_siclom_api",
        alias="carga_viral",
        tags=["siclom"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}

with
    source as (select * from {{ source("brutos_siclom_api_staging", "cargaviral") }})

    select 
        {{ process_null('instituicao_solicitante') }} as instituicao_solicitante,
        {{ process_null('CNES') }} as cnes,
        {{ process_null('cidade_instituicao_solicitante') }} as instituicao_solicitante_cidade,
        {{ process_null('uf_instituicao_solicitante') }} as instituicao_solicitante_uf,
        {{ process_null('cpf') }} as cpf,
        {{ process_null('CNS') }} as cns,
        {{ process_null('nome_completo') }} as paciente_nome,
        {{ process_null('nome_social') }} as paciente_nome_social,
        {{ process_null('nm_mae') }} as mae_nome,
        {{ process_null('data_nascimento') }} as data_nascimento,
        {{ process_null('Sexo') }} as sexo,
        {{ process_null('ds_raca') }} as raca,
        {{ process_null('ds_genero') }} as genero,
        {{ process_null('ds_orientacao_sexual') }} as orientacao_sexual,
        {{ process_null('ds_escolaridade') }} as escolaridade,
        {{ process_null('municipio') }} as municipio,
        {{ process_null('cd_uf') }} as uf,
        {{ process_null('Gestante') }} as gestante,
        {{ process_null('nu_idade_gestacional') }} as nu_idade_gestacional, 
        {{ process_null('co_motivo_exame') }} as codigo_motivo_exame,
        {{ process_null('ds_motivo_exame') }} as motivo_exame,
        {{ process_null('solic_geno_simultanea') }} as solic_geno_simultanea,
        {{ process_null('Data_coleta') }} as coleta_datahora,
        {{ process_null('Data_resultado') }} as resultado_datahora,
        {{ process_null('carga_viral') }} as carga_viral,
        {{ process_null('LOG') }} as log,
        {{ process_null('Condicao_chegada_amostra') }} as condicao_chegada_amostra,
        {{ process_null('extracted_at') }} as extraido_em,
        {{ process_null('data_particao') }} as data_particao,
        cast( cpf as int64) as cpf_particao
    from source