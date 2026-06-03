-- noqa: disable=LT08

-- Regras de EXCLUSÃO da população-alvo do monitoramento de câncer de mama,
-- derivadas do histórico de eventos em mart_monitora_cancer__fatos.
--
-- Granularidade: 1 linha por paciente_cpf excluído, com um array dos motivos
-- aplicáveis (um paciente pode satisfazer mais de uma regra ao mesmo tempo).
--
-- A regra de óbito (cadastral) é aplicada na entrada do funil, em
-- int_monitora_cancer__populacao_alvo; aqui ficam SOMENTE regras derivadas
-- de eventos.
--
-- Estrutura do modelo (lê-se de cima para baixo como uma narrativa):
--
--   1. eventos              → um registro por evento do paciente, com a
--                             "data de referência" (máx entre as datas)
--                             e o ranking de recência (1 = mais recente).
--
--   2. eventos_com_sinais   → para cada evento, calcula flags booleanas
--                             auto-explicativas (eh_mamografia,
--                             resultados_em_cat_3, sem_lesao, etc.).
--                             Toda a complexidade sintática mora aqui.
--
--   3. ultimo / penultimo   → pega os 2 eventos mais recentes por paciente.
--
--   4. motivos              → aplica as regras: cada regra é uma única
--                             linha de WHERE em cima das flags prontas,
--                             fácil de auditar.
--
-- Regras implementadas:
--   (ii)  MAMOGRAFIA_BIRADS_1_OU_2       — último = mamografia Cat 1/2.
--   (iii) MAMOGRAFIA_BIRADS_3_EM_DOIS    — últimos 2 = mamografia Cat 3.
--   (iv)  BIOPSIA_SEM_LESAO              — último = biópsia sem lesão.
--   (v)   SER_ANTIGO                     — último = SER em status terminal
--                                          (CHEGADA_CONFIRMADA / CANCELADA / ALTA)
--                                          há tempo suficiente sem novo evento.

-- Janelas, em meses, usadas na regra (v). Parametrizadas via vars do dbt.
-- A janela depende do evento_status do último evento SER:
--   • CHEGADA_CONFIRMADA / CANCELADA → exclusao_ser_chegada_cancelada_meses
--   • ALTA                           → exclusao_ser_alta_meses
{% set exclusao_ser_chegada_cancelada_meses = var('exclusao_ser_chegada_cancelada_meses', 6) %}
{% set exclusao_ser_alta_meses = var('exclusao_ser_alta_meses', 3) %}


with
    -- ── 1. Eventos com data de referência e ranking por recência ────────────
    eventos as (
        select
            paciente_cpf,
            sistema_origem,
            procedimento,
            evento_status,
            mama_esquerda_resultado,
            mama_direita_resultado,

            data_solicitacao,
            data_autorizacao,
            data_execucao,
            data_exame_resultado,

            -- "data_referencia_evento" = a data mais recente entre as
            -- disponíveis (mesmo critério usado em eventos_episodios).
            (
                select max(d)
                from unnest ([
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_exame_resultado
                ]) as d
            ) as data_referencia_evento
        from {{ ref("mart_monitora_cancer__fatos") }}
        where paciente_cpf is not null
    ),

    eventos_ranqueados as (
        select
            *,
            -- recencia = 1 é o evento MAIS recente, 2 é o penúltimo, etc.
            row_number() over (
                partition by paciente_cpf
                -- mesma ordem canônica usada nos outros modelos do projeto,
                -- só que descendente para pegar os mais recentes primeiro.
                order by
                    data_referencia_evento desc,
                    data_solicitacao desc,
                    data_autorizacao desc,
                    data_execucao desc,
                    data_exame_resultado desc
            ) as recencia
        from eventos
    ),

    -- ── 2. Sinais por evento ────────────────────────────────────────────────
    -- Cada flag abaixo responde a uma pergunta clínica simples em TRUE/FALSE.
    -- Toda a complexidade (prefixos BI-RADS, nulls nas mamas, etc.) fica
    -- encapsulada aqui; as regras lá embaixo só consultam as flags.
    eventos_com_sinais as (
        select
            paciente_cpf,
            recencia,
            data_referencia_evento,

            -- Que TIPO de evento é este?
            sistema_origem = 'SISCAN'
            and procedimento in (
                'RESULTADO MAMOGRAFIA DE RASTREIO',
                'RESULTADO MAMOGRAFIA DIAGNOSTICA'
            ) as eh_mamografia,

            sistema_origem = 'SISCAN'
            and procedimento not in (
                'RESULTADO MAMOGRAFIA DE RASTREIO',
                'RESULTADO MAMOGRAFIA DIAGNOSTICA'
            ) as eh_biopsia,

            sistema_origem = 'SER' as eh_ser,

            -- QUE RESULTADO a mamografia trouxe?
            -- "Todos os resultados preenchidos estão em Cat 1 ou 2"
            -- (ao menos uma mama precisa ter resultado).
            (
                mama_esquerda_resultado is not null
                or mama_direita_resultado is not null
            )
            and (
                mama_esquerda_resultado is null
                or starts_with(mama_esquerda_resultado, 'Categoria 1')
                or starts_with(mama_esquerda_resultado, 'Categoria 2')
            )
            and (
                mama_direita_resultado is null
                or starts_with(mama_direita_resultado, 'Categoria 1')
                or starts_with(mama_direita_resultado, 'Categoria 2')
            ) as resultados_em_cat_1_ou_2,

            -- "Todos os resultados preenchidos estão em Cat 3"
            (
                mama_esquerda_resultado is not null
                or mama_direita_resultado is not null
            )
            and (
                mama_esquerda_resultado is null
                or starts_with(mama_esquerda_resultado, 'Categoria 3')
            )
            and (
                mama_direita_resultado is null
                or starts_with(mama_direita_resultado, 'Categoria 3')
            ) as resultados_em_cat_3,

            -- Biópsia SEM conclusão anátomo-patológica em nenhum lado.
            -- (em int_monitora_cancer__siscan_histo_mama, mama_{lado}_resultado
            -- = COALESCE(lesao_neoplasico, lesao_benigno) — NULL em ambos
            -- significa "nem neoplásica nem benigna").
            mama_esquerda_resultado is null
            and mama_direita_resultado is null as sem_lesao,

            -- Evento SER em status terminal há tempo suficiente sem novo evento.
            -- A janela depende do status; demais status nunca disparam exclusão.
            case
                when evento_status in ('CHEGADA_CONFIRMADA', 'CANCELADA')
                    then data_referencia_evento <= date_sub(
                        current_date('America/Sao_Paulo'),
                        interval {{ exclusao_ser_chegada_cancelada_meses }} month
                    )
                when evento_status = 'ALTA'
                    then data_referencia_evento <= date_sub(
                        current_date('America/Sao_Paulo'),
                        interval {{ exclusao_ser_alta_meses }} month
                    )
                else false
            end as ser_em_status_e_idade_de_exclusao
        from eventos_ranqueados
    ),

    -- ── 3. Dois eventos mais recentes, separados ────────────────────────────
    ultimo as (
        select * from eventos_com_sinais where recencia = 1
    ),

    penultimo as (
        select * from eventos_com_sinais where recencia = 2
    ),

    -- ── 4. Regras ───────────────────────────────────────────────────────────
    -- Cada regra vira um WHERE direto sobre as flags. Leia em português:
    --   "o último evento é mamografia E os resultados estão em Cat 1 ou 2".
    motivos as (

        -- (ii) último evento = mamografia com todos resultados em Cat 1 ou 2
        select
            paciente_cpf,
            'MAMOGRAFIA_BIRADS_1_OU_2' as motivo_exclusao
        from ultimo
        where eh_mamografia
            and resultados_em_cat_1_ou_2

        union all

        -- (iii) os DOIS últimos eventos foram mamografia com Cat 3 em cada
        select
            u.paciente_cpf,
            'MAMOGRAFIA_BIRADS_3_EM_DOIS' as motivo_exclusao
        from ultimo as u
            join penultimo as p using (paciente_cpf)
        where u.eh_mamografia
            and u.resultados_em_cat_3
            and p.eh_mamografia
            and p.resultados_em_cat_3

        union all

        -- (iv) último evento = biópsia sem lesão neoplásica nem benigna
        select
            paciente_cpf,
            'BIOPSIA_SEM_LESAO' as motivo_exclusao
        from ultimo
        where eh_biopsia
            and sem_lesao

        union all

        -- (v) último evento = SER em status terminal há tempo suficiente:
        --   • CHEGADA_CONFIRMADA / CANCELADA → ≥ {{ exclusao_ser_chegada_cancelada_meses }} meses
        --   • ALTA                           → ≥ {{ exclusao_ser_alta_meses }} meses
        --   • demais status (EM_FILA, CHEGADA_NAO_CONFIRMADA, PENDENTE,
        --     AGENDADA)                      → NÃO excluem
        select
            paciente_cpf,
            'SER_ANTIGO' as motivo_exclusao
        from ultimo
        where eh_ser
            and ser_em_status_e_idade_de_exclusao
    )

select
    paciente_cpf,
    array_agg(
        distinct motivo_exclusao
        order by motivo_exclusao
    ) as motivos_exclusao
from motivos
group by paciente_cpf
