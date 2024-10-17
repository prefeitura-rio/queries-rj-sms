-- esta tabela é responsável por armazenar as estatísticas de processamento do
-- histórico clínico
{{
    config(
        schema="gerenciamento__historico_clinico",
        alias="processamento_estatisticas",
    )
}}
with
    -- import CTEs
    -- -- Episodio Assistencial
    -- -- -- Vitacare
    episodio_contagem as (
        select *
        from {{ ref("int_gerenciamento__historico_clinico__episodio_contagem") }}
    ),

    metricas as (select * from episodio_contagem)

select *
from metricas
