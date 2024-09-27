{{
    config(
        schema="brutos_sheets",
        alias="estabelecimento_auxiliar",
    )
}}


with
    source as (
        select * from {{ source("brutos_sheets_staging", "estabelecimento_auxiliar") }}
    )

select
    -- Primary key
    format("%07d", cast(id_cnes as int64)) as id_cnes,  -- fix cases where 0 on the left is lost

    -- Common fields
    if(agrupador_sms = "nan", null, agrupador_sms) as agrupador_sms,
    if(tipo_sms = "nan", null, tipo_sms) as tipo_sms,
    if(tipo_sms_simplificado = "nan", null, tipo_sms_simplificado) as tipo_sms_simplificado,
    safe_cast(nome_fantasia as string) as nome_fantasia,
    if(nome_limpo = "nan", null, nome_limpo) as nome_limpo,
    if(nome_sigla = "nan", null, nome_sigla) as nome_sigla,
    if(area_programatica = "nan", null, cast(area_programatica as string)) as area_programatica,
    if(prontuario_tem = "nan", null, prontuario_tem) as prontuario_tem,
    if(prontuario_versao = "nan", null, prontuario_versao) as prontuario_versao,
    if(prontuario_estoque_tem_dado = "nan", null, prontuario_estoque_tem_dado) as prontuario_estoque_tem_dado,
    if(prontuario_estoque_motivo_sem_dado = "nan", null, prontuario_estoque_motivo_sem_dado) as prontuario_estoque_motivo_sem_dado,
    if(responsavel_sms = "nan", null, responsavel_sms) as responsavel_sms,
    if(administracao = "nan", null, administracao) as administracao,

from source

