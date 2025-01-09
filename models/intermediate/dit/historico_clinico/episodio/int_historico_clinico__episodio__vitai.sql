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

    alta_adm as ( -- Alta administrativa (consultas)
        select 
            gid_boletim,
            CASE
                WHEN 
                    {{ process_null("alta_tipo_detalhado") }} is null and {{clean_abe_obs('abe_obs')}} is null THEN null 
                ELSE
                    CONCAT(
                        IF({{ process_null("alta_tipo_detalhado") }} is null,'',upper(trim({{ process_null("alta_tipo_detalhado") }}))),
                        '\n',
                        IF({{clean_abe_obs('abe_obs')}} is null,'',upper(trim({{clean_abe_obs('abe_obs')}})))
                    ) 
            END as desfecho_atendimento,
        from {{ ref("raw_prontuario_vitai__alta") }}
        qualify row_number() over ( partition by gid_boletim order by datahora desc) = 1
    ),
    alta_internacao as ( -- Resumo de alta (internação)
        select
            resumo_alta.gid_boletim,
            resumo_alta.resumo_alta_datahora,
            concat(
                IF(
                    {{ process_null("resumo_alta.desfecho_internacao") }} is null,
                    '',
                    upper(trim({{ process_null("resumo_alta.desfecho_internacao") }}))
                ),
                '\n',
                IF(
                    (resumo_alta.resumo_alta_descricao is null) 
                    or (lower(trim(resumo_alta.resumo_alta_descricao)) in ('acima','anexo','no prontuário','no prontuario')), 
                    '',
                    upper(trim({{process_null("resumo_alta.resumo_alta_descricao") }}))
                )
            ) as desfecho_atendimento
        from {{ ref("raw_prontuario_vitai__resumo_alta") }} as resumo_alta
        qualify row_number() over ( partition by resumo_alta.gid_boletim order by resumo_alta.resumo_alta_datahora desc) = 1
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
        select gid as id_paciente, cpf, cns, data_nascimento
        from
            {{ ref("raw_prontuario_vitai__paciente") }}
    ),
    boletim as (
        select
            b.gid,
            b.id_hci,
            b.gid_paciente,
            b.gid_estabelecimento,
            estabelecimento_nome,
            b.atendimento_tipo,
            b.especialidade_nome,
            case
                when {{ process_null("b.internacao_data") }} is null
                then null
                else cast(b.internacao_data as datetime)
            end as internacao_data,
            b.imported_at,
            b.updated_at,
            case
                when {{ process_null("b.data_entrada") }} is null
                then null
                else cast(b.data_entrada as datetime)
            end as entrada_datahora,
            case
                when {{ process_null("b.alta_data") }} is null
                then null
                else cast(b.alta_data as datetime)
            end as saida_datahora,
            if(
                {{ clean_numeric("b.cpf") }} is null,
                paciente_mrg.cpf,
                {{ clean_numeric("b.cpf") }}
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
    -- Prescrições VITAI
    prescricoes_limpo as (
        select distinct
            gid_prescricao,
            gid_boletim, 
            CASE 
                WHEN regexp_contains(upper(item_prescrito), 'MEDICAMENTO N[Ã|A]O PADRONIZADO') THEN upper(observacao)
                ELSE upper(item_prescrito)
            END as nome,
            quantidade,
            unidade_medida,
            CASE 
                WHEN regexp_contains(upper(item_prescrito), 'MEDICAMENTO N[Ã|A]O PADRONIZADO') THEN null
                ELSE coalesce(upper(observacao),upper(orientacao_uso))
            END as uso,
            via_administracao
        from {{ ref("raw_prontuario_vitai__basecentral__item_prescricao") }} 
        where trim(tipo_produto) = 'MEDICACAO'
    ),
    prescricao_datahora as (
        select distinct 
            gid, 
            data_prescricao 
        from {{ ref("raw_prontuario_vitai__basecentral__prescricao") }}
    ),
    prescricoes_agg as (
        select 
            gid_boletim, 
            array_agg(
                struct(
                    nome,
                    quantidade,
                    unidade_medida,
                    uso,
                    via_administracao,
                    data_prescricao as prescricao_data
                )
            ) as medicamentos_administrados
        from prescricoes_limpo
        left join prescricao_datahora
            on gid = gid_prescricao
        group by 1
    ),
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Tabela de consultas
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Se o ultimo atendimento conter as informações de queixa do primeiro, puxamos apenas o ultimo. Caso contrário, concatenamos o primeiro e o segundo 
    queixas_all as 
    (
    select
        gid_boletim,
        trim(upper(queixa)) as queixa,
        inicio_datahora,
        gid_profissional,
        row_number() over (
            partition by gid_boletim order by inicio_datahora desc
        ) as ordenacao_desc,
        row_number() over (
            partition by gid_boletim order by inicio_datahora asc
        ) as ordenacao_asc,
    FROM {{ref('raw_prontuario_vitai__atendimento')}}
    order by 1
    ),
    queixa_final as (
    select distinct 
        boletins.gid_boletim, 
        queixa_final.gid_profissional,
        CASE -- compara os caracteres que deveriam ser da queixa inicial e calcula se distancia de Levenshtein é pequena o suficiente
        WHEN edit_distance(left(queixa_final.queixa,char_length(queixa_inicial.queixa)),queixa_inicial.queixa) <= (char_length(queixa_inicial.queixa))/2
            THEN upper(trim(queixa_final.queixa))
        ELSE CONCAT(upper(trim(queixa_inicial.queixa)),'\n',upper(trim(queixa_final.queixa)))
        END as queixa
    from 
        (select distinct gid_boletim from queixas_all ) as boletins
    left join (
        select gid_boletim, queixa, gid_profissional
        from queixas_all 
        where ordenacao_desc = 1
    ) as queixa_final
    on queixa_final.gid_boletim = boletins.gid_boletim
    left join (
        select gid_boletim, queixa
        from queixas_all 
        where ordenacao_asc = 1
    ) as queixa_inicial
    on queixa_inicial.gid_boletim = boletins.gid_boletim

    ),
    -- Monta estrutura array aninhada de profissionais do episódio
    profissional_distinct as (
        select distinct
            queixa_final.gid_boletim as gid_boletim,
            queixa_final.gid_profissional as profissional_id,
            {{ clean_numeric("profissional.cns") }} as profissional_cns,
            {{ clean_numeric("profissional.cpf") }} as profissional_cpf,
            {{ process_null("profissional.nome") }} as profissional_nome,
            profissional.cbo_descricao
        from queixa_final
        left join profissional on queixa_final.gid_profissional = profissional.gid
    ),
    -- Tabela final de consulta
    consulta as (
        select
            boletim.*,
            'Consulta' as tipo,
            struct(
                profissional_distinct.profissional_id as id,
                profissional_distinct.profissional_cpf as cpf,
                profissional_distinct.profissional_cns as cns,
                {{ proper_br("profissional_distinct.profissional_nome") }} as nome,
                profissional_distinct.cbo_descricao as especialidade
            ) as profissional_saude_responsavel,
            {{ process_null("atendimento.cid_codigo") }} as cid_codigo,
            {{ process_null("atendimento.cid_nome") }} as cid_nome,
            coalesce(alta_adm.desfecho_atendimento, alta_internacao.desfecho_atendimento) as desfecho_atendimento,
            case
                when trim(lower(boletim.atendimento_tipo)) = 'emergencia'
                then 'Emergência'
                when trim(lower(boletim.atendimento_tipo)) = 'consulta'
                then 'Ambulatorial'
                else null
            end as subtipo,
            array<struct<tipo string, descricao string>>[] as exames_realizados
        from boletim
        left join alta_adm 
            on boletim.gid = alta_adm.gid_boletim 
        left join alta_internacao 
            on boletim.gid = alta_internacao.gid_boletim 
        left join
            {{ ref("raw_prontuario_vitai__atendimento") }} as atendimento
            on boletim.gid = atendimento.gid_boletim
        left join
            profissional_distinct on profissional_distinct.gid_boletim = boletim.gid
        where atendimento.gid_boletim is not null and boletim.internacao_data is null
    ),
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Tabela de internações
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Alguns hospitais podem por pacientes em obs antes de internar, gerando
    -- duplicadas na tabela. Vale o ultimo registros nesse caso
    internacao_all as (
        select
            gid_boletim,
            internacao_tipo,
            {{ process_null("i.id_diagnostico") }} as cid_codigo,
            {{ process_null("i.diagnostico_descricao") }} as cid_nome,
            {{ process_null("i.gid_profissional") }} as gid_profissional,
            initcap({{ process_null("i.profissional_nome") }}) as profissional_nome,
            profissional_int.cns as profissional_cns,
            profissional_int.cpf as profissional_cpf,
            profissional_int.cbo_descricao as profissional_cbo,
            row_number() over (
                partition by gid_boletim order by internacao_data desc
            ) as ordenacao
        from {{ ref("raw_prontuario_vitai__internacao") }} as i
        left join profissional_int on profissional_int.gid = i.gid_profissional
    ),
    internacao as (
        select
            boletim.*,
            'Internação' as tipo,
            struct(
                internacao_distinct.gid_profissional as id,
                internacao_distinct.profissional_cpf as cpf,
                internacao_distinct.profissional_cns as cns,
                {{ proper_br("internacao_distinct.profissional_nome") }} as nome,
                internacao_distinct.profissional_cbo as especialidade
            ) as profissional_saude_responsavel,
            internacao_distinct.cid_codigo,
            internacao_distinct.cid_nome,
            regexp_replace(
                        regexp_replace(
                            coalesce(alta_internacao.desfecho_atendimento,alta_adm.desfecho_atendimento),
                            '[Ó|O]BITO *\n[Ó|O]BITO$',
                            'OBITO'
                        ),
                        'TRANSFER[E|Ê]NCIA {1,}\nTRANSF[E|Ê]RENCIA',
                        'TRANSFERÊNCIA'
            ) as desfecho_atendimento,
            case
                when trim(lower(internacao_distinct.internacao_tipo)) = 'emergencia'
                then 'Emergência'
                else trim(initcap(internacao_distinct.internacao_tipo))
            end as subtipo,
            array<struct<tipo string, descricao string>>[] as exames_realizados
        from boletim
        left join
            (select * from internacao_all where ordenacao = 1) internacao_distinct
            on boletim.gid = internacao_distinct.gid_boletim
        left join
            alta_internacao
            on alta_internacao.gid_boletim = boletim.gid
        left join
            alta_adm
            on alta_adm.gid_boletim = boletim.gid
        where
            internacao_distinct.gid_boletim is not null
            and boletim.internacao_data is not null
    ),
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Tabela de exames
    -- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    -- Monta relação de exames em cada episódio, retirando duplicadas de exames
    -- refeitos e agrupando episodios com exames de imagem e laboratorio
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
        left join
            (
                select distinct gid_boletim, tipo, exame_descricao
                from {{ ref("raw_prontuario_vitai__exame") }}
            ) as exame_table
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
            id_hci,
            gid_paciente,
            gid_estabelecimento,
            estabelecimento_nome,
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
                array_agg(distinct subtipo order by subtipo desc), ' e '
            ) as subtipo,
            array_agg(
                struct(
                    cast(subtipo as string) as tipo,
                    cast(exame_descricao as string) as descricao
                )
            ) as exames_realizados
        from exame_dupl
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19
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
            IF(episodios.cid_codigo is null, c.id, episodios.cid_codigo) as cid_id,
            episodios.cid_nome as cid_nome,
        from episodios
        left join (
            select id, descricao
            from {{ ref('dim_condicao_cid10') }} 
        ) as c
        on c.descricao = episodios.cid_nome
        where (cid_codigo is not null or cid_nome is not null)
    ),
    cid_grouped as (
        select
            id,
            array_agg(
                struct(
                    cid_id as id,
                    cid_nome as descricao,
                    "ATIVO" as situacao,
                    "" as data_diagnostico
                ) ignore nulls
            ) as condicoes,
        from cid_distinct
        group by 1
    ),
    -- Monta base do episódio para ser enriquecida
    atendimento_struct as (
        select
            id_hci,
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
                coalesce({{ proper_estabelecimento("estabelecimentos.nome_estabelecimento") }},
                {{ proper_estabelecimento("episodios_distinct.estabelecimento_nome") }}) as nome,
                estabelecimentos.tipo_sms_simplificado as estabelecimento_tipo
            ) as estabelecimento,
            struct(
                episodios_distinct.gid as id_prontuario_global, "vitai" as fornecedor
            ) as prontuario,
            episodios_distinct.imported_at,
            episodios_distinct.updated_at,
            case
                when
                    (episodios_distinct.cpf is null)
                    and (episodios_distinct.cns is null)
                then 0
                else 1
            end as episodio_com_paciente

        from
            (
                select distinct
                    id_hci,
                    gid,
                    gid_estabelecimento,
                    estabelecimento_nome,
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
            id_hci,
            -- Paciente
            atendimento_struct.paciente.cpf as cpf,

            -- Tipo e Subtipo
            safe_cast(atendimento_struct.tipo as string) as tipo,
            safe_cast(atendimento_struct.subtipo as string) as subtipo,
            exames_realizados,

            -- Entrada e Saída
            safe_cast(
                atendimento_struct.entrada_datahora as datetime
            ) as entrada_datahora,
            safe_cast(atendimento_struct.saida_datahora as datetime) as saida_datahora,

            -- Motivo e Desfecho
            safe_cast(
                trim(atendimento_struct.motivo_atendimento) as string
            ) as motivo_atendimento,
            safe_cast(trim(desfecho_atendimento) as string) as desfecho_atendimento,

            -- Condições
            cid_grouped.condicoes,

            -- Medicamentos administrados
            prescricoes_agg.medicamentos_administrados,

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
        left join prescricoes_agg on atendimento_struct.id = prescricoes_agg.gid_boletim

    )

select *
from final
