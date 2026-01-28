{{
    config(
        schema="brutos_siclom_api",
        alias="linfocitos_cd4",
        tags=["siclom"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}

with 

source as (
    select * from {{ source('brutos_siclom_api_staging', 'linfocitos_cd4') }}
)

select 
    {{ process_null('cidade_instituicao_solicitante') }} as instituicao_solicitante_cidade,
    {{ process_null('CNES') }} as cnes,
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
    {{ process_null('avaliacao_inicial') }} as avaliacao_inicial,
    {{ process_null('mo_pes_assinto_segmento') }} as mo_pes_assinto_segmento, -- Confirmar nome da coluna
    {{ process_null('mo_crianca_adolescente') }} as mo_crianca_adolescente, -- Confirmar nome da coluna
    {{ process_null('mo_pes_falha_viro') }} as mo_pes_falha_viro, -- Confirmar nome da coluna
    {{ process_null('mo_pes_sinto') }} as mo_pes_sinto, -- Confirmar nome da coluna
    {{ process_null('avaliacao_imuniza') }} as avaliacao_imuniza, -- Confirmar nome da coluna
    {{ process_null('nao_informado') }} as nao_informado,
    {{ process_null('instituicao_solicitante') }} as instituicao_solicitante,
    {{ process_null('dt_hr_coleta') }} as coleta_datahora,
    {{ process_null('dt_result') }} as resultado_datahora,
    {{ process_null('contagem_cd3') }} as contagem_cd3,
    {{ process_null('contagem_cd4') }} as contagem_cd4,
    {{ process_null('contagem_cd8') }} as contagem_cd8,
    {{ process_null('extracted_at') }} as extraido_em,
    cast( cpf as int64) as cpf_particao
from source