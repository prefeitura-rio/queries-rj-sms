{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_vitai",
        materialized="table",
    )
}}
with
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Tabelas uteis para o episodio
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    -- Desfecho do atendimento
    alta_adm as (
        select 
        gid_boletim, 
        alta_tipo_detalhado,
        row_number() over (
            partition by gid_boletim order by alta_data desc
        ) as ordenacao
        from {{ ref("raw_prontuario_vitai__alta") }}
    ),
    desfecho_atendimento_all as (
        select
            resumo_alta.gid_boletim,
            resumo_alta.resumo_alta_datahora,
            {{process_null('resumo_alta.resumo_alta_descricao')}} as resumo_alta_descricao,
            {{process_null('resumo_alta.desfecho_internacao')}} as resumo_alta_tipo,
            row_number() over (
                partition by resumo_alta.gid_boletim 
                order by resumo_alta.resumo_alta_datahora desc
            ) as ordenacao
        from {{ ref("raw_prontuario_vitai__resumo_alta") }}
    ),
    desfecho_atendimento_final as (
        select gid_boletim, 
            case 
                when resumo_alta_descricao is null then resumo_alta_tipo 
                when resumo_alta_tipo is null then resumo_alta_descricao 
                else concat(
                    trim(upper(resumo_alta_tipo)),
                    '\n',
                    trim(upper(resumo_alta_descricao))
                )
            end as desfecho_atendimento
        from desfecho_atendimento_all
        where ordenacao = 1
    ),
    -- Estabelecimento com infos da tabela mestre
    estabelecimentos as (
        select
            gid,
            cnes,
            estabelecimento_dim.nome_limpo as nome_estabelecimento,
            estabelecimento_dim.tipo_sms_simplificado
        from
            {{ ref("raw_prontuario_vitai__m_estabelecimento") }}
            as estabelecimento_vitai
        left join
            {{ ref("dim_estabelecimento") }} as estabelecimento_dim
            on estabelecimento_vitai.cnes = estabelecimento_dim.id_cnes
    ),
    -- Boletim (centralizador do episodio) com cpf enriquecido
    paciente_mrg as (
        select
            id_paciente,
            cpf,
            cns,
            dados.data_nascimento
        from {{ ref('mart_historico_clinico__paciente') }} as paciente_merged, 
            unnest(prontuario) as prontuario
        where sistema = 'VITAI'
    ),
    boletim as (
        select
            b.gid,
            b.gid_paciente,
            b.gid_estabelecimento,
            b.atendimento_tipo,
            b.especialidade_nome,
            case
                when {{process_null('b.internacao_data')}} is null 
                then null
                else cast(b.internacao_data as datetime)
            end as internacao_data,
            b.imported_at,
            b.updated_at,
            case
                when {{process_null('b.data_entrada')}} is null 
                then null
                else cast(b.data_entrada as datetime)
            end as entrada_datahora,
            case
                when {{process_null('b.alta_data')}} is null 
                then null
                else cast(b.alta_data as datetime)
            end as saida_datahora,
            IF(
                {{clean_numeric('b.cpf')}} is null, 
                paciente_mrg.cpf,
                {{clean_numeric('b.cpf')}}
            ) as cpf,
            paciente_mrg.cns as cns,
            paciente_mrg.data_nascimento
        from {{ ref("raw_prontuario_vitai__boletim") }} as b
        left join paciente_mrg on b.gid_paciente = paciente_mrg.id_paciente
    ),
    -- Profissional com nome próprio tratado
    profissional_int as (
        select gid, cns, cpf, initcap(nome) as nome, cbo_descricao
        from {{ ref("raw_prontuario_vitai__profissional") }}
    ),
    profissional as (
        select gid, cns, cpf, {{ proper_br("nome") }} as nome, cbo_descricao
        from profissional_int
    ),
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Tabela de consultas
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Como cada atendimento appenda informações no boletim, pegamos a queixa do
    -- ultimo atendimento
    queixa_all as (
        select
            gid_boletim,
            queixa,
            inicio_datahora,
            gid_profissional,
            row_number() over (
                partition by gid_boletim order by inicio_datahora desc
            ) as ordenacao
        from {{ ref("raw_prontuario_vitai__atendimento") }}
    ),
    queixa_final as (
        select
            gid_boletim,
            gid_profissional,
            case
                when {{process_null('queixa')}} is null 
                then null
                else upper(trim(queixa))
            end as queixa
        from queixa_all
        where ordenacao = 1
    ),
    -- Monta estrurra array aninhada de profissionais do episódio
    profissional_distinct as (
        select distinct
            queixa_final.gid_boletim as gid_boletim,
            queixa_final.gid_profissional as profissional_id,
            {{clean_numeric('profissional.cns')}} as profissional_cns,
            {{clean_numeric('profissional.cpf')}} as profissional_cpf,
            {{process_null('profissional.nome')}} as profissional_nome,
            profissional.cbo_descricao
        from queixa_final
        left join profissional on queixa_final.gid_profissional = profissional.gid
    ),
    consulta as (
        select
            boletim.*,
            'Consulta' as tipo,
            struct(
                profissional_distinct.profissional_id as id,
                profissional_distinct.profissional_cpf as cpf,
                profissional_distinct.profissional_cns as cns,
                {{proper_br('profissional_distinct.profissional_nome')}} as nome,
                profissional_distinct.cbo_descricao as especialidade
            ) as profissional_saude_responsavel,
            {{process_null('atendimento.cid_codigo')}} as cid_codigo,
            {{process_null('atendimento.cid_nome')}}  as cid_nome,
            alta_adm.alta_tipo_detalhado  as desfecho_atendimento,
            case 
                when trim(lower(boletim.atendimento_tipo)) = 'emergencia' THEN 'Emergência'
                when trim(lower(boletim.atendimento_tipo)) = 'consulta' THEN 'Ambulatorial'
                else null
            end  as subtipo,
            array(
                select as struct 
                cast(null as string) as tipo,
                cast(null as string) as descricao
            ) as exames_realizados
        from boletim
        left join
            {{ ref("raw_prontuario_vitai__atendimento") }} as atendimento
            on boletim.gid = atendimento.gid_boletim
        left join profissional_distinct 
            on profissional_distinct.gid_boletim = boletim.gid
        left join (select * from alta_adm where ordenacao=1) as alta_adm
            on alta_adm.gid_boletim = boletim.gid
        where atendimento.gid_boletim is not null and boletim.internacao_data is null
    ),
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Tabela de internações
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    
    -- Alguns hospitais podem por pacientes em obs antes de internar, gerando duplicadas na tabela. Vale o ultimo registros nesse caso
    internacao_all as (
        select
            gid_boletim,  
            internacao_tipo,
            {{process_null('i.id_diagnostico')}} as cid_codigo,
            {{process_null('i.diagnostico_descricao')}} as cid_nome,
            {{process_null('i.gid_profissional')}} as gid_profissional,
            initcap({{process_null('i.profissional_nome')}}) as profissional_nome,
            profissional_int.cns as profissional_cns,
            profissional_int.cpf as profissional_cpf,
            profissional_int.cbo_descricao as profissional_cbo,
            row_number() over (
                partition by gid_boletim order by internacao_data desc
            ) as ordenacao
        from {{ ref("raw_prontuario_vitai__internacao") }} as i
        left join profissional_int 
        on profissional_int.gid = i.gid_profissional
    ),
    internacao as (
        select
            boletim.*,
            'Internação' as tipo,
            struct(
                internacao_distinct.gid_profissional as id,
                internacao_distinct.profissional_cpf as cpf,
                internacao_distinct.profissional_cns as cns,
                {{proper_br('internacao_distinct.profissional_nome')}} as nome,
                internacao_distinct.profissional_cbo as especialidade
            ) as profissional_saude_responsavel,
            internacao_distinct.cid_codigo,
            internacao_distinct.cid_nome,
            regexp_replace(
                regexp_replace(
                    desfecho_atendimento,
                    '[Ó|O]BITO {1,}\n[Ó|O]BITO',
                    'OBITO'
                ),
                'TRANSFER[E|Ê]NCIA {1,}\nTRANSF[E|Ê]RENCIA', 
                'TRANSFERÊNCIA'
            ) as desfecho_atendimento,
            case 
                when trim(lower(internacao_distinct.internacao_tipo)) = 'emergencia' THEN 'Emergência'
                else trim(initcap(internacao_distinct.internacao_tipo)) 
            end as subtipo,
            array(
                select as struct 
                cast(null as string) as tipo,
                cast(null as string) as descricao
            ) as exames_realizados
        from boletim
        left join ( select * from internacao_all where ordenacao=1) internacao_distinct
            on boletim.gid = internacao_distinct.gid_boletim
        left join desfecho_atendimento_final
            on desfecho_atendimento_final.gid_boletim = boletim.gid
        where internacao_distinct.gid_boletim is not null and boletim.internacao_data is not null
    ),
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Tabela de exames
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    -- Monta relação de exames em cada episódio, retirando duplicadas de exames refeitos e agrupando episodios com exames de imagem e laboratorio
    -- como um só
    exame_dupl as (
        select
            boletim.*,
            'Exame' as tipo,            
            struct(
                safe_cast(null as string) as id,
                safe_cast(null as string) as cpf,
                safe_cast(null as string) as cns,
                safe_cast(null as string) as nome,
                safe_cast(null as string) as especialidade
                ) as profissional_saude_responsavel,
            exame_table.exame_descricao,
            safe_cast(null as string) as cid_codigo,
            safe_cast(null as string) as cid_descricao,
            case 
                when trim(lower(exame_table.tipo)) = 'laboratorio' 
                then 'Laboratório'
                else trim(initcap(exame_table.tipo)) 
            end as subtipo
        from boletim
        left join (select distinct gid_boletim, tipo, exame_descricao  from {{ref("raw_prontuario_vitai__exame")}} ) as exame_table
            on boletim.gid = exame_table.gid_boletim
        left join
            {{ ref("raw_prontuario_vitai__atendimento") }} as atendimento
            on boletim.gid = atendimento.gid_boletim
        where
            exame_table.gid_boletim is not null
            and atendimento.gid_boletim is null
            and boletim.internacao_data is null
    ),
    exame as (
        select
            gid,
            gid_paciente,
            gid_estabelecimento,
            atendimento_tipo,
            especialidade_nome,
            internacao_data,
            imported_at,
            updated_at,
            entrada_datahora,
            saida_datahora,
            cpf,
            cns,
            data_nascimento,
            tipo,
            profissional_saude_responsavel,
            cid_codigo,
            cid_descricao,
            safe_cast(null as string) as desfecho_atendimento,
            array_to_string(
                array_agg(distinct subtipo order by subtipo desc),
                ' e ') as subtipo,
            array_agg(
                struct( 
                    cast(subtipo as string) as tipo,
                    cast(exame_descricao as string) as descricao
                )
            ) as exames_realizados
        from exame_dupl
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
    ),
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Montagem do episódio e enriquecimento
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    episodios as (
        select *
        from consulta
        union all
        select *
        from internacao
        union all
        select *
        from exame
    ),
    -- Monta estrura array aninhada de CIDs do episódio
    cid_distinct as (
        select distinct
            episodios.gid as id,
            episodios.cid_codigo as cid_id,
            episodios.cid_nome as cid_nome,
        from episodios
    ),
    cid_grouped as (
        select
            id,
            array_agg(
                struct(cid_id as id, cid_nome as descricao) ignore nulls
            ) as condicoes,
        from cid_distinct
        group by 1
    ),
    -- Monta base do episódio para ser enriquecida
    atendimento_struct as (
        select
            episodios_distinct.gid as id,
            queixa_final.queixa as motivo_atendimento,
            episodios_distinct.tipo,
            episodios_distinct.subtipo,
            episodios_distinct.exames_realizados,
            episodios_distinct.desfecho_atendimento,
            episodios_distinct.entrada_datahora,
            episodios_distinct.saida_datahora,
            struct(
                episodios_distinct.cpf, 
                episodios_distinct.cns,
                episodios_distinct.data_nascimento
            ) as paciente,
            profissional_saude_responsavel,
            struct(
                estabelecimentos.cnes as id_cnes,
                {{ proper_estabelecimento("nome_estabelecimento") }} as nome,
                estabelecimentos.tipo_sms_simplificado as estabelecimento_tipo
            ) as estabelecimento,
            struct(
                episodios_distinct.gid as id_atendimento, 
                "vitai" as fornecedor
            ) as prontuario,
            episodios_distinct.imported_at,
            episodios_distinct.updated_at,
            case
                when (episodios_distinct.cpf is null) 
                and (episodios_distinct.cns is null) 
                then 0 
                else 1
            end as episodio_com_paciente

        from (
            select distinct gid, 
            gid_estabelecimento,
            tipo,
            subtipo,
            profissional_saude_responsavel,
            exames_realizados,
            entrada_datahora,
            saida_datahora,
            imported_at,
            updated_at,
            cpf,
            cns,
            data_nascimento,
            desfecho_atendimento
            from episodios
        ) as episodios_distinct
        left join 
            estabelecimentos 
            on episodios_distinct.gid_estabelecimento = estabelecimentos.gid
        left join queixa_final on episodios_distinct.gid = queixa_final.gid_boletim
    ),
    final as (
        select    
            -- Paciente
            atendimento_struct.paciente,

            -- Tipo e Subtipo
            safe_cast(atendimento_struct.tipo as string) as tipo,
            safe_cast(atendimento_struct.subtipo as string) as subtipo,
            exames_realizados,

            -- Entrada e Saída
            safe_cast(atendimento_struct.entrada_datahora as datetime) as entrada_datahora,
            safe_cast(atendimento_struct.saida_datahora as datetime) as saida_datahora,

            -- Motivo e Desfecho
            safe_cast(atendimento_struct.motivo_atendimento as string) as motivo_atendimento,
            safe_cast(desfecho_atendimento as string) as desfecho_atendimento,
            
            -- Condições
            cid_grouped.condicoes,

            -- Estabelecimento
            atendimento_struct.estabelecimento,

            -- Profissional
            atendimento_struct.profissional_saude_responsavel,

            -- Prontuário
            atendimento_struct.prontuario,

            -- Metadados
            struct(
                safe_cast(updated_at as datetime) as updated_at,
                safe_cast(imported_at as datetime) as imported_at,
                safe_cast(current_datetime() as datetime) as processed_at
            ) as metadados,
            safe_cast(entrada_datahora as date) as data_particao,
            safe_cast(atendimento_struct.paciente.cpf as int64) as cpf_particao
            from atendimento_struct
            left join cid_grouped on atendimento_struct.id = cid_grouped.id
    )
    select * from final