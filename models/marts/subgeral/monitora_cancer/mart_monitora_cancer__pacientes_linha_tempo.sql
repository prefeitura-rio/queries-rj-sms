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
        select
            paciente_cpf,
            case
                when max(cast(criterio_diagnostico as int64)) = 1 then 'DIAGNOSTICO'
                else 'SUSPEITA'
            end as status
        from {{ ref("mart_monitora_cancer__fatos") }}
        where
            data_solicitacao >= "2025-01-01"
            and (
                criterio_suspeita = true
                or criterio_diagnostico = true
            )
        group by paciente_cpf
    ),

    -- to do: pegar o registro mais recente ao invés de safe_offset(0)
    enriquece_populacao_interesse as (
        select
            pop.paciente_cpf,
            pop.status,

            dim_paciente.nomes[SAFE_OFFSET(0)] as nome,
            dim_paciente.racas_cores[SAFE_OFFSET(0)] as raca_cor,

            -- dim_paciente.telefones[SAFE_OFFSET(0)] as telefone,

            date_diff(
                current_date(),
                safe_cast(dim_paciente.datas_nascimento[SAFE_OFFSET(0)] as date),
                year
            ) as idade,

            dim_paciente.clinicas_sf[SAFE_OFFSET(0)] as clinica_sf,
            dim_paciente.clinicas_sf_ap[SAFE_OFFSET(0)] as clinica_sf_ap,
            dim_paciente.clinicas_sf_telefone[SAFE_OFFSET(0)] as clinica_sf_telefone,
            dim_paciente.equipes_sf[SAFE_OFFSET(0)] as equipe_sf,
            -- dim_paciente.equipes_sf_telefone[SAFE_OFFSET(0)] as equipe_sf_telefone,

            dsr.dias_sem_resposta as gravidade_score,

            exists (
                select 1
                from unnest(dim_paciente.anos_obito) as ano
                where ano is not null
            ) as obito_indicador

        from populacao_interesse as pop 

        left join {{ref("dim_paciente__subgeral")}} as dim_paciente
        on pop.paciente_cpf = dim_paciente.cpf_particao

        left join {{ref("mart_monitora_cancer__pacientes_dias_sem_resposta")}} as dsr
        on pop.paciente_cpf = safe_cast(dsr.paciente_cpf as int)

        where dim_paciente.sexos[SAFE_OFFSET(0)] != "MASCULINO"
        having obito_indicador = false
    ),

    eventos as (
        select
            lpad(safe_cast(fcts.paciente_cpf as string), 11, '0') as cpf,
            fcts.paciente_cpf as cpf_particao,

            -- dados basicos paciente
            pop.nome,
            pop.raca_cor,
            pop.idade,
            pop.clinica_sf_ap as ap,
            pop.clinica_sf as cf,
            pop.equipe_sf,
            pop.status,
            pop.gravidade_score,
            pop.clinica_sf_telefone as telefone,

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
