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
                when max(case when sistema_origem = 'SER' then 1 else 0 end) = 1 then 'Em tratamento na UNACON'
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
            fcts.mama_direita_classif_radiologica,
            fcts.evento_status,
            (
                select max(d)
                from unnest([
                    fcts.data_solicitacao,
                    fcts.data_autorizacao,
                    fcts.data_execucao,
                    fcts.data_exame_resultado
                ]) as d
            ) as data_referencia_evento

        from enriquece_populacao_interesse as pop
            left join {{ref("mart_monitora_cancer__fatos")}} as fcts
            on pop.paciente_cpf = fcts.paciente_cpf
    ),

    eventos_com_proximo as (
        select
            *,
            date_diff(
                lead(data_referencia_evento) over (
                    partition by cpf_particao
                    order by
                        data_solicitacao,
                        data_autorizacao,
                        data_execucao,
                        data_resultado
                ),
                data_referencia_evento,
                day
            ) as dias_proximo_evento
        from eventos
    ),

    eventos_com_lag as (
        select
            *,
            lag(data_referencia_evento) over (
                partition by cpf_particao
                order by
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado
            ) as data_referencia_evento_anterior
        from eventos_com_proximo
    ),

    eventos_com_run as (
        select
            *,
            sum(
                case
                    when data_referencia_evento_anterior is null then 1
                    when date_diff(
                        data_referencia_evento,
                        data_referencia_evento_anterior,
                        day
                    ) > 180 then 1
                    else 0
                end
            ) over (
                partition by cpf_particao
                order by
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado
                rows between unbounded preceding and current row
            ) as run_id
        from eventos_com_lag
    ),

    run_starts as (
        select
            cpf_particao,
            run_id,
            min(data_solicitacao) as run_start_data
        from eventos_com_run
        group by cpf_particao, run_id
    ),

    primeira_ser_info as (
        select
            cpf_particao,
            data_solicitacao as primeira_ser_data,
            run_id as primeira_ser_run_id
        from eventos_com_run
        where fonte = 'SER'
        qualify row_number() over (
            partition by cpf_particao
            order by
                data_solicitacao,
                data_autorizacao,
                data_execucao,
                data_resultado
        ) = 1
    ),

    tempo_total_por_paciente as (
        select
            psi.cpf_particao,
            cast(
                date_diff(psi.primeira_ser_data, rs.run_start_data, day)
                as int64
            ) as tempo_total
        from primeira_ser_info as psi
        join run_starts as rs
            on psi.cpf_particao = rs.cpf_particao
            and psi.primeira_ser_run_id = rs.run_id
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
                    evento_status,
                    procedimento,
                    cid,

                    unidade_solicitante,
                    unidade_executante,

                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado,

                    safe_cast(data_solicitacao as string) as data_solicitacao_str,
                    safe_cast(data_autorizacao as string) as data_autorizacao_str,
                    safe_cast(data_execucao as string) as data_execucao_str,
                    safe_cast(data_resultado as string) as data_resultado_str,


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
                    ) as resultados,

                    dias_proximo_evento
                )

                order by
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado
            ) as eventos,

            any_value(ttp.tempo_total) as tempo_total

        from eventos_com_proximo
        left join tempo_total_por_paciente as ttp
            using (cpf_particao)
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
