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
cluster_by = ['status', 'cf', 'equipe_sf', 'cpf_particao'],
on_schema_change = 'sync_all_columns'
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
            bcadastro.nome,
            
            dim_paciente.racas_cores [SAFE_OFFSET(0)] as raca_cor,

            coalesce(
                concat(
                    coalesce(trim(contato.telefone.ddi), null),
                    coalesce(trim(contato.telefone.ddd), null),
                    coalesce(trim(contato.telefone.numero), null)
                ),
                telefones.`telefones`[SAFE_OFFSET(0)].telefone_formatado
            ) as telefone,

            date_diff(
                current_date(),
                bcadastro.nascimento_data,
                year
            ) as idade,

            dim_paciente.clinicas_sf [SAFE_OFFSET(0)] as clinica_sf,
            dim_paciente.clinicas_sf_ap [SAFE_OFFSET(0)] as clinica_sf_ap,
            dim_paciente.clinicas_sf_telefone [SAFE_OFFSET(0)] as clinica_sf_telefone,
            dim_paciente.equipes_sf [SAFE_OFFSET(0)] as equipe_sf,
            dim_paciente.equipes_sf_telefone[SAFE_OFFSET(0)] as equipe_sf_telefone,

            dsr.dias_sem_resposta as gravidade_score

        from populacao_interesse as pop

            left join {{ref("pacientes_subgeral__dim_paciente")}} as dim_paciente
            on pop.paciente_cpf = dim_paciente.cpf_particao

            left join {{ref("mart_monitora_cancer__pacientes_dias_sem_resposta")}} as dsr
            on pop.paciente_cpf = safe_cast(dsr.paciente_cpf as int)

            left join {{ref("mart_iplanrio__telefones_validos")}} as telefones
            on pop.paciente_cpf = safe_cast(telefones.cpf as int)

            left join {{ref("raw_bcadastro__cpf")}} as bcadastro
            on pop.paciente_cpf = bcadastro.cpf_particao

        where bcadastro.sexo != "masculino"
            and bcadastro.obito_ano is null
            and not exists(
                select 1
                from unnest (dim_paciente.anos_obito) as ano
                where ano is not null
            )
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
            pop.telefone,
            pop.clinica_sf_telefone as telefone_cf,
            pop.equipe_sf_telefone as telefone_esf,     

            -- dados evento
            fcts.sistema_origem as fonte,
            fcts.sistema_tipo as tipo,
            fcts.procedimento,
            fcts.cid,
            concat(
                fcts.id_cnes_unidade_origem,
                " - ",
                fcts.estabelecimento_origem_nome
            ) as unidade_solicitante,
            concat(
                fcts.id_cnes_unidade_executante,
                " - ",
                fcts.estabelecimento_executante_nome
            ) as unidade_executante,
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
            telefone_cf,
            telefone_esf,

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
            telefone,
            telefone_cf,
            telefone_esf
    )

select *
from paciente_linha_tempo
