-- esta tabela é responsável por armazenar as estatísticas de processamento do
-- histórico clínico

with
    -- import CTEs
    -- -- Episodio Assistencial
    -- -- -- Vitacare

    raw_episodio_vitacare as (
        select * from {{ ref("raw_prontuario_vitacare__atendimento") }}
    ),

    int_episodio_vitacare as (
        select * from {{ ref("int_historico_clinico__episodio__vitacare") }}
    ),

    mrg_episodio_vitacare as (
        select *
        from {{ ref("mart_historico_clinico__episodio") }}
        where prontuario.fornecedor = "vitacare"
    ),

    app_episodio_vitacare as (
        select *
        from {{ ref("mart_historico_clinico_app__episodio") }}
        where provider = "vitacare"
    ),

    -- -- -- Vitai
    raw_episodio_vitai as (
        select * from {{ ref("raw_prontuario_vitai__atendimento") }}
    ),

    int_episodio_vitai as (
        select * from {{ ref("int_historico_clinico__episodio__vitai") }}
    ),

    mrg_episodio_vitai as (
        select *
        from {{ ref("mart_historico_clinico__episodio") }}
        where prontuario.fornecedor = "vitai"
    ),

    app_episodio_vitai as (
        select *
        from {{ ref("mart_historico_clinico_app__episodio") }}
        where provider = "vitai"
    ),

    -- logical CTEs
    -- -- Vitacare
    raw_episodio_vitacare_stats as (
        select
            "vitacare" as prontuario,
            "episodio" as entidade,
            "raw" as etapa,
            "qtd_registros" as metrica,
            count(*) as total_registros,
        from raw_episodio_vitacare
    ),

    int_episodio_vitacare_stats as (
        select
            "vitacare" as prontuario,
            "episodio" as entidade,
            "int" as etapa,
            "qtd_registros" as metrica,
            count(*) as total_registros
        from int_episodio_vitacare
    ),

    mrg_episodio_vitacare_stats as (
        select
            "vitacare" as prontuario,
            "episodio" as entidade,
            "mrg" as etapa,
            "qtd_registros" as metrica,
            count(*) as total_registros
        from mrg_episodio_vitacare
    ),

    app_episodio_vitacare_stats as (
        select
            "vitacare" as prontuario,
            "episodio" as entidade,
            "app" as etapa,
            "qtd_registros" as metrica,
            count(*) as total_registros
        from app_episodio_vitacare
    ),

    -- -- Vitai
    raw_episodio_vitai_stats as (
        select
            "vitai" as prontuario,
            "episodio" as entidade,
            "raw" as etapa,
            "qtd_registros" as metrica,
            count(*) as total_registros
        from raw_episodio_vitai
    ),

    int_episodio_vitai_stats as (
        select
            "vitai" as prontuario,
            "episodio" as entidade,
            "int" as etapa,
            "qtd_registros" as metrica,
            count(*) as total_registros
        from int_episodio_vitai
    ),

    mrg_episodio_vitai_stats as (
        select
            "vitai" as prontuario,
            "episodio" as entidade,
            "mrg" as etapa,
            "qtd_registros" as metrica,
            count(*) as total_registros
        from mrg_episodio_vitai
    ),

    app_episodio_vitai_stats as (
        select
            "vitai" as prontuario,
            "episodio" as entidade,
            "app" as etapa,
            "qtd_registros" as metrica,
            count(*) as total_registros
        from app_episodio_vitai
    ),

    -- final CTE
    episodio_stats as (
        select *
        from raw_episodio_vitacare_stats
        union all
        select *
        from int_episodio_vitacare_stats
        union all
        select *
        from mrg_episodio_vitacare_stats
        union all
        select *
        from app_episodio_vitacare_stats
        union all
        select *
        from raw_episodio_vitai_stats
        union all
        select *
        from int_episodio_vitai_stats
        union all
        select *
        from mrg_episodio_vitai_stats
        union all
        select *
        from app_episodio_vitai_stats
    ),

    episodio_stats_pivoted as (
                select *
                from
                    episodio_stats pivot (
                        sum(total_registros) for etapa in ('raw', 'int', 'mrg', 'app')
                    )
    )

-- final CTE
-- simple select statement
select *
from episodio_stats_pivoted
