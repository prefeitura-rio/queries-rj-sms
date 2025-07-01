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
    safe_cast(formaarticula as string) as forma_articulacao,
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast(indchangepac as string) as evolucao_articulacao
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_articula') }}