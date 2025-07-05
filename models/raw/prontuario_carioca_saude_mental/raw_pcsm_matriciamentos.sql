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
    safe_cast(formamatric as string) as forma_matriciamento,
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast(indchangepac as string) as evolucao_matriciamento
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_matric') }}