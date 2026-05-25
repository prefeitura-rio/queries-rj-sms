-- noqa: disable=LT08

{{
  config(
    enabled=true,
    materialized='table',
    schema="pacientes_subgeral",
    alias="dim_paciente_subgeral",
    unique_key='cpf',
    partition_by={
        "field": "cpf_particao",
        "data_type": "int64",
        "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
    on_schema_change='sync_all_columns',
    tags=["weekly"]
  )
}}

with
-- cada linha é um registro de um paciente em um sistema de origem (já deduplicado
-- upstream para 1 linha por paciente por sistema, com data_atualizacao do registro).
registros_base as (
    select *
    from {{ref("pacientes_subgeral__cadastros")}}
    where paciente_cpf is not null
),

-- todos os valores já vistos em qualquer sistema.
-- útil para detectar discrepâncias entre fontes (ex.: nomes ligeiramente diferentes
-- ou datas de nascimento divergentes em sistemas distintos).
historico as (
    select
        cpf_particao,

        array_agg(distinct sistema_origem ignore nulls) as sistemas_origem,
        count(*) as n_registros,

        array_agg(distinct paciente_cns ignore nulls) as cns_lista,

        array_agg(distinct paciente_nome ignore nulls) as nomes,
        array_agg(distinct paciente_nome_mae ignore nulls) as nomes_mae,
        array_agg(distinct paciente_nome_social ignore nulls) as nomes_sociais,
        array_agg(distinct cast(paciente_data_nascimento as string) ignore nulls) as datas_nascimento,
        array_agg(distinct paciente_sexo ignore nulls) as sexos,
        array_agg(distinct paciente_racacor ignore nulls) as racas_cores,
        array_agg(distinct paciente_obito_ano ignore nulls) as anos_obito

    from registros_base
    group by cpf_particao
),

-- VÍNCULO APS: sempre do HCI
vinculo_aps as (
    select
        cpf_particao,
        clinica_sf,
        clinica_sf_ap,
        clinica_sf_telefone,
        equipe_sf,
        equipe_sf_telefone
    from registros_base
    where sistema_origem = 'hci'
),

-- bcadastro é usado apenas para enriquecimento de atributos pessoais: lê somente
-- pacientes já descobertos em outras fontes (inner join), evitando varrer a tabela
-- inteira da Receita Federal. não contribui com novas linhas em registros_base.
enriquecimento_bcadastro as (
    select
        rb.cpf_particao,
        bc.paciente_nome,
        bc.paciente_nome_mae,
        bc.paciente_nome_social,
        bc.paciente_data_nascimento,
        bc.paciente_sexo,
        bc.paciente_obito_ano,
        bc.data_atualizacao
    from (select distinct cpf_particao from registros_base) as rb
    inner join {{ref("int_dim_paciente__pacientes_bcadastro")}} as bc
        on rb.cpf_particao = bc.paciente_cpf
),

-- ATRIBUTOS PESSOAIS PRINCIPAIS
-- regra: prioridade fixa receita_federal (bcadastro) > hci > minha_saude > resto.
-- desempate por data_atualizacao mais recente.
-- bcadastro não está em registros_base (não é fonte de descoberta de pacientes),
-- mas participa do ranking via enriquecimento_bcadastro (inner join).
-- cada campo é escolhido INDEPENDENTEMENTE: se a fonte vencedora tiver null naquele
-- campo, o valor cai para a próxima fonte da ordem (via first_value ignore nulls).
-- consequência: campos diferentes do mesmo paciente podem vir de fontes distintas.
atributos_pessoais_rankeados as (
    select
        cpf_particao,
        paciente_nome,
        paciente_nome_mae,
        paciente_nome_social,
        paciente_data_nascimento,
        paciente_sexo,
        paciente_racacor,
        paciente_obito_ano,
        data_atualizacao,
        case sistema_origem
            when 'hci' then 2
            when 'minha_saude' then 3
            else 4
        end as prioridade_pessoal
    from registros_base

    union all

    -- receita_federal: prioridade máxima para atributos pessoais.
    -- nota: bcadastro não tem raca_cor - esse campo virá da próxima fonte disponível.
    select
        cpf_particao,
        paciente_nome,
        paciente_nome_mae,
        paciente_nome_social,
        paciente_data_nascimento,
        paciente_sexo,
        cast(null as string) as paciente_racacor,
        paciente_obito_ano,
        data_atualizacao,
        1 as prioridade_pessoal
    from enriquecimento_bcadastro
),

atributos_pessoais as (
    select distinct
        cpf_particao,

        first_value(paciente_nome ignore nulls) over w as nome,
        first_value(paciente_nome_mae ignore nulls) over w as nome_mae,
        first_value(paciente_nome_social ignore nulls) over w as nome_social,
        first_value(paciente_data_nascimento ignore nulls) over w as data_nascimento,
        first_value(paciente_sexo ignore nulls) over w as sexo,
        first_value(paciente_racacor ignore nulls) over w as raca_cor,
        first_value(paciente_obito_ano ignore nulls) over w as obito_ano

    from atributos_pessoais_rankeados
    window w as (
        partition by cpf_particao
        order by prioridade_pessoal asc, data_atualizacao desc nulls last
        rows between unbounded preceding and unbounded following
    )
),

-- 1 linha por paciente, atributos escalares + arrays de histórico
final as (
    select
        lpad(safe_cast(h.cpf_particao as string), 11, '0') as cpf,
        h.cpf_particao,

        -- atributos pessoais escolhidos pela regra de prioridade.
        ap.nome,
        ap.nome_mae,
        ap.nome_social,
        ap.data_nascimento,
        ap.sexo,
        ap.raca_cor,
        ap.obito_ano,

        -- vínculo APS: sempre do HCI. NULL quando o paciente não está no HCI.
        v.clinica_sf,
        v.clinica_sf_ap,
        v.clinica_sf_telefone,
        v.equipe_sf,
        v.equipe_sf_telefone,

        -- metadados de proveniência.
        h.sistemas_origem,
        h.n_registros,
        h.cns_lista,

        -- arrays de histórico para auditoria (todos os valores já vistos em qualquer sistema).
        h.nomes,
        h.nomes_mae,
        h.nomes_sociais,
        h.datas_nascimento,
        h.sexos,
        h.racas_cores,
        h.anos_obito

    from historico as h
    left join atributos_pessoais as ap
        on h.cpf_particao = ap.cpf_particao
    left join vinculo_aps as v
        on h.cpf_particao = v.cpf_particao
)

select * from final
