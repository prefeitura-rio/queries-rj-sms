-- noqa: disable=LT08
-- Fatos unificados do monitoramento de câncer de mama.
-- Responsável pelo UNION das fontes intermediárias + enriquecimento de CPF e estabelecimento.

-- Lookback empírico: p99 do date_diff(data_solicitacao, data_execucao) sobre todas
-- as solicitações no sisreg (histórico) em 04/05/2026.
-- Cobre eventos cuja execução chega tardiamente após a solicitação.
{% set lookback_dias = 464 %}

{% set last_partition = get_last_partition_date( this ) %}

{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    schema="projeto_monitora_cancer",
    alias="fatos",
    unique_key=['sistema_origem', 'id_sistema_origem'],
    partition_by={
      "field": "data_solicitacao",
      "data_type": "date",
      "granularity": "month",
    },
incremental_predicates=[
  "DBT_INTERNAL_DEST.data_solicitacao >= date_sub(date('" ~ last_partition ~ "'), interval " ~ lookback_dias ~ " day)"
],
cluster_by=['sistema_origem', 'id_cnes_unidade_origem', 'id_cnes_unidade_executante', 'paciente_cpf'],
on_schema_change='sync_all_columns'
)
}}

with
    -- Snapshot da última competência do dim de estabelecimentos.
    -- Não usa modelo DIM da DIT pois temos mais unidades.
    dim_estabelecimento_atual as (
        select id_cnes, nome_fantasia
        from {{ ref('dim_estabelecimento_sus_rio_historico') }}
        qualify rank() over (order by ano_competencia desc, mes_competencia desc) = 1
    ),

    fontes_unificadas as (
        select * from {{ ref("int_monitora_cancer__sisreg") }}
            {% if is_incremental() %}
        where data_solicitacao >= date_sub('{{ last_partition }}', interval {{ lookback_dias }} day)
            {% endif %}

        union all

        select * from {{ ref("int_monitora_cancer__ser_ambulatorial") }}
            {% if is_incremental() %}
        where data_solicitacao >= date_sub('{{ last_partition }}', interval {{ lookback_dias }} day)
            {% endif %}

        union all

        select * from {{ ref("int_monitora_cancer__siscan") }}
            {% if is_incremental() %}
        where data_solicitacao >= date_sub('{{ last_partition }}', interval {{ lookback_dias }} day)
            {% endif %}

        union all

        select * from {{ ref("int_monitora_cancer__siscan_histo_mama") }}
            {% if is_incremental() %}
        where data_solicitacao >= date_sub('{{ last_partition }}', interval {{ lookback_dias }} day)
            {% endif %}
    ),

    transforma as (
        select
            sistema_origem,
            sistema_tipo,
            id_sistema_origem,
            paciente_cns,
            paciente_cpf_sisreg,
            id_cnes_unidade_origem,
            id_cnes_unidade_executante,
            left(cid, 3) as cid,
            evento_status,
            procedimento,
            data_solicitacao,
            data_autorizacao,
            data_execucao,
            data_exame_resultado,
            mama_esquerda_resultado,
            mama_direita_resultado,
            criterio_suspeita,
            criterio_diagnostico,
            atraso_solicitacao_autorizacao,
            atraso_autorizacao_execucao,
            atraso_regulacao
        from fontes_unificadas
    ),

    enriquece_cpf as (
        -- CPF via lookup CNS→CPF; fallback para o CPF direto do SISREG quando disponível
        select
            coalesce(safe_cast(cns_cpf.cpf as int), transforma.paciente_cpf_sisreg) as paciente_cpf,
            transforma.*

        from transforma
            left join {{ ref("pacientes_subgeral__relacao_cns_cpf") }} as cns_cpf
            on safe_cast(transforma.paciente_cns as int) = safe_cast(cns_cpf.cns as int)
    ),

    enriquece_estabelecimento as (
        select
            enriquece_cpf.*,
            estabs_origem.nome_fantasia as estabelecimento_origem_nome,
            estabs_exec.nome_fantasia as estabelecimento_executante_nome

        from enriquece_cpf

            left join dim_estabelecimento_atual as estabs_origem
            on safe_cast(id_cnes_unidade_origem as int) = safe_cast(estabs_origem.id_cnes as int)

            left join dim_estabelecimento_atual as estabs_exec
            on safe_cast(id_cnes_unidade_executante as int) = safe_cast(estabs_exec.id_cnes as int)
    ),

    final as (
        select
        -- pk
            sistema_origem,
            id_sistema_origem,

        -- id paciente
            paciente_cpf,

        -- tipo sistema fonte
            sistema_tipo,

        -- qualificacao do procedimento
            procedimento,
            cid,
            evento_status,

        -- datas
            data_solicitacao,
            data_autorizacao,
            data_execucao,

        -- resultados siscan
            data_exame_resultado,
            mama_esquerda_resultado,
            mama_direita_resultado,

        -- indicadores
            criterio_suspeita,
            criterio_diagnostico,

        -- atraso (severidade 0-3)
            atraso_solicitacao_autorizacao,
            atraso_autorizacao_execucao,
            atraso_regulacao,

        -- unidade solicitante
            id_cnes_unidade_origem,
            estabelecimento_origem_nome,

        -- unidade executante
            id_cnes_unidade_executante,
            estabelecimento_executante_nome
        from enriquece_estabelecimento
    )

select * from final
