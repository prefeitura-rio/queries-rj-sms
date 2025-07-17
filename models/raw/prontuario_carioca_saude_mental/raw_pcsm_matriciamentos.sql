{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="matriciamentos",
        materialized="table",
        tags=["raw", "pcsm", "matriciamento"],
        description="Matriciamentos feitos em unidades de saúde psico-sociais da Prefeitura do Rio de Janeiro. Matriciamento é uma estratégia de organização do cuidado em saúde mental baseada na interdisciplinaridade e na articulação em rede."
    )
}}

select
    safe_cast(seqmatric as int64) as id_matriciamento,
    safe_cast(nmmatric as string) as nome_matriciamento,
    safe_cast(dtmatric as date) as data_inicio_matriciamento,
    safe_cast(horamatric as string) as hora_inicio_matriciamento,
    safe_cast(tpmatric as string) as tipo_matriciamento,
    case trim(safe_cast(tpmatric as string))
        when 'U' then 'Urgência/Emergência/Hospitalar'
        when 'A' then 'Atenção básica'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tipo_matriciamento,
    safe_cast(formamatric as string) as forma_matriciamento,
    case trim(safe_cast(formamatric as string))
        when 'P' then 'Presencial'
        when 'T' then 'Telefônico'
        when 'V' then 'Vídeo'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_forma_matriciamento,
    safe_cast(sequs as int64) as id_unidade_saude,
    case trim(safe_cast(indchangepac as string))
        when 'y' then 'S'
        when 'Y' then 'S' 
        when 'n' then 'N'
        when '' then 'N'
        when null then 'N'
        else trim(safe_cast(indchangepac as string))
    end as evolucao_matriciamento,
    case trim(safe_cast(indchangepac as string))
        when 'Y' then 'Sim'
        when 'y' then 'Sim'
        when 'N' then 'Não'
        when 'n' then 'Não'
        when '' then 'Não'
        when null then 'Não'
        else 'Não classificado'
    end as descricao_evolucao_matriciamento,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_matric') }}