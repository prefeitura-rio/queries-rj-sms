{{ 
    config(
        materialized="table",
        alias="sih"
    )
}}

with
    -- ------------------------------------------------------------------------------------------------
    -- Base de Dados
    -- ------------------------------------------------------------------------------------------------
    pacientes_base as (
        select distinct
            id_hash as id_base,
            paciente_cpf as cpf_base,
            {{ clean_name_string("upper(paciente_nome)") }} as nome_base,
            {{ clean_name_string("upper(paciente_mae_nome)") }} as nome_mae_base,
            cast(paciente_data_nascimento as date) as data_nascimento_base,
        from {{ ref("raw_sih__autorizacoes_internacoes_hospitalares") }}
    ),
    pacientes_hci as (
        select
            safe_cast(cpf as string) as cpf_fonte,
            {{ clean_name_string("upper(dados.nome)") }} as nome_fonte,
            {{ clean_name_string("upper(dados.mae_nome)") }} as nome_mae_fonte,
            dados.data_nascimento as data_nascimento_fonte
        from {{ ref("mart_historico_clinico__paciente") }}
        where
            dados.nome is not null
            and dados.mae_nome is not null
            and dados.data_nascimento is not null
    ),
    -- ------------------------------------------------------------------------------------------------
    -- Identificando Casos
    -- ------------------------------------------------------------------------------------------------
    caso_1_com_cpf as (
        select
            *
        from pacientes_base
        where
            cpf_base is not null
    ),
    caso_2_sem_cpf_com_dados_fuzzy as (
        select
            *
        from pacientes_base
        where
            cpf_base is null 
            and (
                nome_base is not null 
                and nome_mae_base is not null 
                and data_nascimento_base is not null
            )
    ),
    caso_3_sem_cpf_sem_dados_suficientes as (
        select
            *
        from pacientes_base
        where
            cpf_base is null 
            and (
                nome_base is null 
                or nome_mae_base is null 
                or data_nascimento_base is null
            )
    ),
    -- ------------------------------------------------------------------------------------------------
    -- Processando Casos
    -- ------------------------------------------------------------------------------------------------
    -- Caso 1: CPF Presente
    caso_1_com_cpf_resultados as (
        select
            id_base,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "cpf_base",
                    ]
                )
            }} as id_paciente,
            'chave-cpf' as tipo_linkage,
            cast(null as float64) as score_final,

            nome_base,
            nome_mae_base,
            cast(data_nascimento_base as date) as data_nascimento_base,
            cpf_base,

            cast(null as string) as nome_fonte,
            cast(null as string) as nome_mae_fonte,
            cast(null as date) as data_nascimento_fonte,
            cast(null as string) as cpf_fonte
        from caso_1_com_cpf
    ),

    -- Caso 2: CPF Não Presente + Campos Nome, Nome da Mãe e Data de Nascimento Presentes
    caso_2_sem_cpf_com_dados_scores as (
        select
            *,

            (
                {{ calculate_lev("nome_base", "nome_fonte") }} + 
                {{ calculate_lev("nome_mae_base", "nome_mae_fonte") }}
            )/2 as score_lev,
            (
                {{ calculate_jaccard("nome_base", "nome_fonte") }} + 
                {{ calculate_jaccard("nome_mae_base", "nome_mae_fonte") }}
            )/2 as score_jac,
            (
                {{ calculate_lev("nome_base", "nome_fonte") }} + 
                {{ calculate_lev("nome_mae_base", "nome_mae_fonte") }} + 
                {{ calculate_jaccard("nome_base", "nome_fonte") }} + 
                {{ calculate_jaccard("nome_mae_base", "nome_mae_fonte") }}
            )/4 as score_final

        from caso_2_sem_cpf_com_dados_fuzzy
            left join pacientes_hci on data_nascimento_base = data_nascimento_fonte
    ),
    caso_2_sem_cpf_com_dados_ranked as (
        select
            *,
            row_number() over (
                partition by id_base
                order by score_final asc
            ) as rn,

            case 
                when score_final = 0 then 'match-exato'
                else 'match-fuzzy' 
            end as tipo_linkage
        from caso_2_sem_cpf_com_dados_scores
    ),
    caso_2_sem_cpf_com_dados_resultados as (
        select
            id_base,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "cpf_fonte",
                    ]
                )
            }} as id_paciente,
            tipo_linkage,
            score_final,

            nome_base,
            nome_mae_base,
            data_nascimento_base,
            cpf_base,

            nome_fonte,
            nome_mae_fonte,
            data_nascimento_fonte,
            cpf_fonte
        from caso_2_sem_cpf_com_dados_ranked
        where rn = 1
    ),

    -- Caso 3: CPF Não Presente + Campos Nome, Nome da Mãe e Data de Nascimento Não Presentes
    caso_3_sem_cpf_sem_dados_suficientes_resultados as (
        select
            id_base,
            cast(null as string) as id_paciente,
            'dados-insuficientes' as tipo_linkage,
            null as score_final,

            nome_base,
            nome_mae_base,
            data_nascimento_base,
            cpf_base,

            cast(null as string) as nome_fonte,
            cast(null as string) as nome_mae_fonte,
            cast(null as date) as data_nascimento_fonte,
            cast(null as string) as cpf_fonte
        from caso_3_sem_cpf_sem_dados_suficientes
    ),
    -- ------------------------------------------------------------------------------------------------
    -- Unindo Casos
    -- ------------------------------------------------------------------------------------------------
    casos_unidos as (
        select * from caso_1_com_cpf_resultados
        union all
        select * from caso_2_sem_cpf_com_dados_resultados
        union all
        select * from caso_3_sem_cpf_sem_dados_suficientes_resultados
    )

select *
from casos_unidos
order by tipo_linkage, score_final asc