{{
    config(
        alias="ludopatia",
        materialized="incremental",
        unique_key = 'id_prontuario_global',
        cluster_by= 'id_prontuario_global',
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with

    source_ludopatia as (
        select
            concat(
                nullif(id_cnes, ''),
                '.',
                nullif(replace(acto_id, '.0', ''), '')
            ) as id_prontuario_global,
            *
        from {{ source('brutos_prontuario_vitacare_historico_staging', 'ludopatia') }}
        {% if is_incremental() %}
            where data_particao > '{{last_partition}}'
        {% endif %}
    ),

    ludopatia_deduplicados as (
        select
            *
        from source_ludopatia
        qualify row_number() over (partition by id_prontuario_global order by extracted_at desc) = 1
    ),

    ludopatia as (
        select
            -- PKs e Chaves
            id_prontuario_global,
            replace(acto_id, '.0', '') as id_prontuario_local,
            id_cnes,
            {{ process_null('ut_id') }} as ut_id,
            {{ process_null('prof_id') }} as id_profissional,

            -- Dados do questionário
            {{ process_null('classificacao') }} as classificacao,
            {{ process_null('ludopiapontuacao') }} as ludopatia_pontuacao,

            -- Perguntas do questionário
            {{ process_null('alguemcriticouapostas') }} as alguem_criticou_apostas,
            {{ process_null('apostarcausouconflitos') }} as apostar_causou_conflitos,
            {{ process_null('apostarcausoudificuldadefinanceiras') }} as apostar_causou_dificuldades_financeiras,
            {{ process_null('apostarcausouproblemasSaude') }} as apostar_causou_problemas_saude,
            {{ process_null('apostouMaisQuePodia') }} as apostou_mais_que_podia,
            {{ process_null('mentiuFamiliaTempoJogos') }} as mentiu_familia_tempo_jogos,
            {{ process_null('necessidadeApostarMais') }} as necessidade_apostar_mais,
            {{ process_null('pediuDinheiroEmprestado') }} as pediu_dinheiro_emprestado,
            {{ process_null('perseguiuPerdas') }} as perseguiu_perdas,
            {{ process_null('precisouApostarMais') }} as precisou_apostar_mais,
            {{ process_null('sentiuCulpa') }} as sentiu_culpa,

            cast({{ process_null('extracted_at') }} as datetime) as loaded_at,
            date(safe_cast(extracted_at as datetime)) as data_particao
        from ludopatia_deduplicados
    )

select
    *
from ludopatia
