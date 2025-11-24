-- noqa: disable=LT08

{{
  config(
    enabled=true,
    schema="projeto_monitora_cancer",
    alias="pacientes_linha_tempo",
    unique_key=['cpf_particao'],
    partition_by={
        "field": "cpf_particao",
        "data_type": "int64",
        "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
    cluster_by=['status', 'cf', 'equipe_sf', 'cpf_particao'],
    on_schema_change='sync_all_columns'
  )
}}

with
    populacao_interesse as (
        select distinct paciente_cpf
        from {{ref ("mart_monitora_cancer__fatos")}}
        where
            data_solicitacao >= "2025-01-01"
            and (
                -- critérios de seleção SISCAN
                (
                    sistema_origem = "SISCAN"
                    and mama_esquerda_classif_radiologica in (
                        "Categoria 4 - achados mamográficos suspeitos",
                        "Categoria 5 - achados mamográficos altamente suspeitos",
                        "Categoria 6 - achados mamográficos"
                    )
                )

                or

                -- critérios de seleção SISREG
                (
                    sistema_origem = "SISREG"
                    and procedimento in (
                        "MAMOGRAFIA  DIAGNOSTICA",
                        "BIOPSIA DE MAMA   LESAO PALPAVEL",
                        "BIOPSIA DE MAMA GUIADA POR USG",
                        "BIOPSIA DE MAMA POR ESTEREOTAXIA",

                        "ULTRASSONOGRAFIA MAMARIA BILATERAL PARA ORIENTAR BIOPSIA DE MAMA"
                    )
                )

                or

                -- critérios de seleção SER
                (
                    sistema_origem = "SER"
                    and procedimento in (
                        "AMBULATORIO 1  VEZ   MASTOLOGIA  ONCOLOGIA"
                    )
                )

            )
    ),

    enriquece_populacao_interesse as (
        select
            pop.paciente_cpf,
            dim_paciente.nomes[SAFE_OFFSET(0)] as nome,
            dim_paciente.racas_cores[SAFE_OFFSET(0)] as raca_cor,
            
            date_diff(
                current_date(),
                safe_cast(dim_paciente.datas_nascimento[SAFE_OFFSET(0)] as date),
                year
            ) as idade,

            dsr.dias_sem_resposta as gravidade_score

        from populacao_interesse as pop 

        left join {{ref("dim_paciente__subgeral")}} as dim_paciente
        on pop.paciente_cpf = dim_paciente.cpf_particao

        left join {{ref("mart_monitora_cancer__pacientes_dias_sem_resposta")}} as dsr
        on pop.paciente_cpf = safe_cast(dsr.paciente_cpf as int)

        where dim_paciente.sexos[SAFE_OFFSET(0)] != "MASCULINO"
    ),

    eventos as (
        select
            lpad(safe_cast(fcts.paciente_cpf as string), 11, '0') as cpf,
            fcts.paciente_cpf as cpf_particao,

            -- dados basicos paciente
            pop.nome,
            pop.raca_cor,
            pop.idade,
            cast(null as string) as ap,
            cast(null as string) as cf,
            cast(null as string) as equipe_sf,
            cast(null as string) as status,
            pop.gravidade_score,
            cast(null as string) as telefone,

            -- dados evento
            fcts.sistema_origem as fonte,
            fcts.sistema_tipo as tipo,
            fcts.procedimento,
            fcts.cid,
            fcts.estabelecimento_origem_nome as unidade_solicitante,
            fcts.estabelecimento_executante_nome as unidade_executante,
            fcts.data_solicitacao,
            fcts.data_autorizacao,
            fcts.data_execucao,
            fcts.data_exame_resultado as data_resultado,
            fcts.mama_esquerda_classif_radiologica,
            fcts.mama_direita_classif_radiologica

        from enriquece_populacao_interesse as pop
        left join {{ref("mart_monitora_cancer__fatos")}} as fcts
        on pop.paciente_cpf = fcts.paciente_cpf
    ),

    paciente_linha_tempo as (
        select
            -- pk
            cpf_particao,
            cpf,

            -- id paciente
            nome,
            raca_cor,
            idade,
            ap,
            cf,
            equipe_sf,

            -- qualificadores gerais
            status,
            gravidade_score,
            count(*) as procedimentos_n,

            -- contato paciente
            telefone,

            -- sistemas com eventos do paciente
            struct(
                logical_or(fonte = 'SISCAN') as siscan,
                logical_or(fonte = 'SER') as ser,
                logical_or(fonte = 'SISREG') as sisreg
            ) as fontes,

            -- eventos
            array_agg (
                struct(
                    fonte,
                    tipo,
                    procedimento,
                    cid,

                    unidade_solicitante,
                    unidade_executante,

                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado,

                    array_concat(
                        if(
                            mama_esquerda_classif_radiologica is null,
                            [],
                            [concat("Mama Esquerda ", mama_esquerda_classif_radiologica)]
                        ),

                        if(
                            mama_direita_classif_radiologica is null,
                            [],
                            [concat("Mama Direita ", mama_direita_classif_radiologica)]
                        )
                    ) as resultados
                )

                order by
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado
            ) as eventos

        from eventos
        group by
            cpf_particao,
            cpf,
            nome,
            raca_cor,
            idade,
            ap,
            cf,
            equipe_sf,
            status,
            gravidade_score,
            telefone
    )

select *
from paciente_linha_tempo
