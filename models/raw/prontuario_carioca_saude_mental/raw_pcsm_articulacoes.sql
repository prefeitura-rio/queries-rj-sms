{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="articulacoes",
        materialized="table",
        tags=["raw", "pcsm", "articulacao"],
        description="Articulações feitas entre unidades de saúde do Rio de Janeiro. Uma articulação é uma reunião de unidades de saúde quando é necessário se trabalhar em conjunto para atingir a um objetivo."
    )
}}

select
    safe_cast(seqarticula as int64) as id_articulacao,
    safe_cast(nmarticula as string) as nome_articulacao,
    safe_cast(dtarticula as date) as data_entrada_articulacao,
    safe_cast(horaarticula as string) as hora_entrada_articulacao,
    safe_cast(tparticula as string) as tipo_articulacao,
    case trim(safe_cast(tparticula as string))
        when 'U' then 'Urgência/Emergência/Hospitalar'
        when 'A' then 'Atenção básica'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tipo_articulacao,
    safe_cast(formaarticula as string) as forma_articulacao,
    case trim(safe_cast(formaarticula as string))
        when 'P' then 'Presencial'
        when 'T' then 'Telefônico'
        when 'V' then 'Vídeo'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_forma_articulacao,
    safe_cast(sequs as int64) as id_unidade_saude,
    case trim(safe_cast(indchangepac as string))
        when 'y' then 'S'
        when 'Y' then 'S' 
        when 'n' then 'N'
        when '' then 'N'
        when null then 'N'
        else trim(safe_cast(indchangepac as string))
    end as evolucao_articulacao,
    case trim(safe_cast(indchangepac as string))
        when 'Y' then 'Sim'
        when 'y' then 'Sim'
        when 'N' then 'Não'
        when 'n' then 'Não'
        when '' then 'Não'
        when null then 'Não'
        else 'Não classificado'
    end as descricao_evolucao_articulacao,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_articula') }}