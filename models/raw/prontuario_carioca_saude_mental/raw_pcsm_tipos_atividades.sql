{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="tipos_atividades",
        materialized="table",
        tags=["raw", "pcsm", "tipo_atividade"],
        description="Tipos possíveis de atividades. Exemplo: Reabilitação psicosocial."
    )
}}

select
    safe_cast(seqtpativ as int64) as id_tipo_atividade,                       -- Identificador único do tipo de atividade
    safe_cast(dsctpativ as string) as descricao_tipo_atividade,               -- Descrição do tipo da atividade
    safe_cast(codproced as int64) as codigo_procedimento,                     -- Código do procedimento
    safe_cast(listcbo as string) as lista_cbos_permitidos,                    -- Lista de CBOs permitidos
    safe_cast(indusopacsemcad as string) as criterio_paciente_sem_cadastro,   -- Indicador de uso do critério de paciente sem cadastro
    case trim(safe_cast(indusopacsemcad as string)) 
        when 'S' then 'Sim' 
        when 'N' then 'Não' 
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_criterio_paciente_sem_cadastro,                          -- Descrição do critério de paciente sem cadastro
    safe_cast(indformafat as string) as forma_faturamento,                    -- Indicador de forma de faturamento
    case trim(safe_cast(indformafat as string)) 
        when 'I' then 'BPA-I'
        when 'C' then 'BPA-C'
        when 'R' then 'RAAS'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_forma_faturamento,                                       -- Descrição da forma de faturamento
    safe_cast(indinativo as string) as status_registro,                       -- Status do registro (S-Ativo, N-Inativo)
    case trim(safe_cast(indinativo as string)) 
        when 'S' then 'Ativo' 
        when 'N' then 'Inativo' 
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_status_registro,                                         -- Descrição do status do registro
    safe_cast({{ process_null('dsclstraps') }} as string) as lista_rede_atencao_habilitado,         -- Lista de pontos de atenção habilitados
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_tpatividades') }}