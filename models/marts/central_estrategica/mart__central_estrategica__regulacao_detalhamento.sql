{{
  config(
    enabled=true,
    alias="regulacao_detalhamento",
    materialized='table',
  )
}}

/*
  Detalhamento das solicitações de regulação ambulatorial.
  Granularidade: uma linha por solicitação (solicitacao_id).

  Campos de identificação do paciente são anonimizados (iniciais do nome e idade).
  As datas de aprovação da marcação e da marcação em si são extraídas do array
  marcacao[], considerando a marcação mais recente com aprovacao_datahora preenchido.

  Fonte: mart_historico_clinico_app__regulacao + mart_regulacao__solicitacao
         + int_regulacao__paciente_sisreg
*/

with
    -- Base: todas as solicitações do app (já sem HIV)
    regulacao as (
        select
            r.solicitacao_id,
            r.solicitacao_datahora,
            r.detalhe_status,
            r.classificacao_risco,
            r.procedimento_descricao,
            r.unidade_solicitante,
            r.marcacao,
            -- CNES do solicitante vem do mart_regulacao__solicitacao
            s.solicitante.unidade_id_cnes                       as cnes_unidade_solicitante,
            -- CNES do executante: pega a última execução com CNES preenchido
            (
                select ex.unidade_id_cnes
                from unnest(s.execucao) as ex
                where ex.unidade_id_cnes is not null
                limit 1
            )                                                   as cnes_unidade_executante,
            (
                select ex.unidade_nome
                from unnest(s.execucao) as ex
                where ex.unidade_id_cnes is not null
                limit 1
            )                                                   as nome_unidade_executante_sisreg,
            s.paciente_cpf,
            s.paciente_cns
        from {{ ref('mart_historico_clinico_app__regulacao') }} as r
        inner join {{ ref('mart_regulacao__solicitacao') }} as s
            on r.solicitacao_id = s.solicitacao.id
    ),

    -- Extrai a marcação mais relevante:
    -- a última marcação com aprovacao_datahora preenchida
    com_marcacao as (
        select
            solicitacao_id,
            solicitacao_datahora,
            detalhe_status,
            classificacao_risco,
            procedimento_descricao,
            unidade_solicitante,
            cnes_unidade_solicitante,
            cnes_unidade_executante,
            nome_unidade_executante_sisreg,
            paciente_cpf,
            paciente_cns,
            (
                select m.aprovacao_datahora
                from unnest(marcacao) as m
                where m.aprovacao_datahora is not null
                order by m.aprovacao_datahora desc
                limit 1
            )                                               as marcacao_aprovacao_datahora,
            (
                select m.datahora
                from unnest(marcacao) as m
                where m.aprovacao_datahora is not null
                order by m.aprovacao_datahora desc
                limit 1
            )                                               as marcacao_datahora
        from regulacao
    ),

    -- Enriquece com dados do paciente (iniciais do nome e idade)
    com_paciente as (
        select
            c.*,
            p.nome                                          as paciente_nome,
            p.nascimento_data                               as paciente_nascimento_data,
            date_diff(current_date(), p.nascimento_data, year) as paciente_idade
        from com_marcacao as c
        left join {{ ref('int_regulacao__paciente_sisreg') }} as p
            on c.paciente_cns = p.cns
    ),

    -- Enriquece com nome oficial das unidades via dim_estabelecimento
    final as (
        select
            cp.solicitacao_id,

            -- Paciente (anonimizado)
            case
                when cp.paciente_nome is not null
                then upper(
                    array_to_string(
                        array(
                            select substr(parte, 1, 1)
                            from unnest(split(trim(cp.paciente_nome), ' ')) as parte
                            where length(trim(parte)) > 1  -- ignora preposições
                        ),
                        '.'
                    )
                ) || '.'
                else null
            end                                                 as paciente_iniciais,
            cp.paciente_idade,

            -- Datas da solicitação
            cp.solicitacao_datahora,
            cp.marcacao_aprovacao_datahora,
            cp.marcacao_datahora,

            -- Classificação
            cp.classificacao_risco,
            cp.detalhe_status,
            cp.procedimento_descricao,

            -- Unidade solicitante
            cp.cnes_unidade_solicitante,
            coalesce(
                dim_sol.nome_acentuado,
                cp.unidade_solicitante
            )                                                   as nome_unidade_solicitante,

            -- Unidade executante
            cp.cnes_unidade_executante,
            coalesce(
                dim_exe.nome_acentuado,
                cp.nome_unidade_executante_sisreg
            )                                                   as nome_unidade_executante

        from com_paciente as cp
        left join {{ ref('dim_estabelecimento') }} as dim_sol
            on cp.cnes_unidade_solicitante = dim_sol.id_cnes
        left join {{ ref('dim_estabelecimento') }} as dim_exe
            on cp.cnes_unidade_executante = dim_exe.id_cnes
    )

select * from final
