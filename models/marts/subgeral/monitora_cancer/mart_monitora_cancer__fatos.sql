-- noqa: disable=LT08
-- Fatos unificados do monitoramento de câncer de mama.
-- Responsável pelo UNION das fontes intermediárias + enriquecimento de CPF e estabelecimento.
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
    cluster_by=['sistema_origem', 'id_cnes_unidade_origem', 'id_cnes_unidade_executante', 'paciente_cpf'],
    on_schema_change='sync_all_columns'
  )
}}

{% set last_partition = get_last_partition_date( this ) %}

{% set cnes_competencia %}
  (select as struct
      div(max_comp, 100) as ano,
      mod(max_comp, 100) as mes
   from (
     select max(ano_competencia * 100 + mes_competencia) as max_comp
     from {{ ref('dim_estabelecimento_sus_rio_historico') }}
   ))
{% endset %}

with
    fontes_unificadas as (
        select * from {{ ref("int_monitora_cancer__sisreg") }}
        {% if is_incremental() %}
        where data_solicitacao >= date_sub('{{ last_partition }}', interval 6 month)
        {% endif %}

        union all

        select * from {{ ref("int_monitora_cancer__ser_ambulatorial") }}
        {% if is_incremental() %}
        where data_solicitacao >= date_sub('{{ last_partition }}', interval 6 month)
        {% endif %}

        union all

        select * from {{ ref("int_monitora_cancer__siscan") }}
        {% if is_incremental() %}
        where data_solicitacao >= date_sub('{{ last_partition }}', interval 6 month)
        {% endif %}

        union all

        select * from {{ ref("int_monitora_cancer__siscan_histo_mama") }}
        {% if is_incremental() %}
        where data_solicitacao >= date_sub('{{ last_partition }}', interval 6 month)
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
            {{ clean_proced_name("procedimento") }} as procedimento,
            data_solicitacao,
            data_autorizacao,
            data_execucao,
            data_exame_resultado,
            mama_esquerda_resultado,
            mama_direita_resultado,
            criterio_suspeita,
            criterio_diagnostico
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

            left join {{ ref("dim_estabelecimento_sus_rio_historico") }} as estabs_origem
            on safe_cast(id_cnes_unidade_origem as int) = safe_cast(estabs_origem.id_cnes as int)

            left join {{ ref("dim_estabelecimento_sus_rio_historico") }} as estabs_exec
            on safe_cast(id_cnes_unidade_executante as int) = safe_cast(estabs_exec.id_cnes as int)

        where 1 = 1
            and estabs_origem.ano_competencia = ({{ cnes_competencia }}).ano
            and estabs_origem.mes_competencia = ({{ cnes_competencia }}).mes
            and estabs_exec.ano_competencia = ({{ cnes_competencia }}).ano
            and estabs_exec.mes_competencia = ({{ cnes_competencia }}).mes
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

        -- unidade solicitante
            id_cnes_unidade_origem,
            estabelecimento_origem_nome,

        -- unidade executante
            id_cnes_unidade_executante,
            estabelecimento_executante_nome
        from enriquece_estabelecimento
    )

select * from final
