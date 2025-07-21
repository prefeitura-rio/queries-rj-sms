with

profissionais_ergon_032025 as (
    select
        f.cpf,
        d.nome as profissional_nome,
        d.provimento_inicio as data_inicio_provimento,
        d.provimento_fim as data_fim_provimento,
        d.setor_sigla,
        d.setor_nome as setor,
        d.cargo_nome as cargo

    from `rj-sms.brutos_ergon.funcionarios` as f
    cross join unnest(f.dados) as d

    where
        -- provimento já iniciado até o último dia de abril
        date(d.provimento_inicio) <= date '2025-03-30'
        and (
        -- provimento não terminou antes de abril (ou está em aberto)
            d.provimento_fim is null
            or
            date(d.provimento_fim) >= date '2025-03-01'
        )
),

ergon_agrupado as (
    select
        cpf,
        array_agg(distinct profissional_nome ignore nulls) as profissional_nome,
--        array_agg(distinct data_inicio_provimento ignore nulls) as data_inicio_provimento,
--        array_agg(distinct data_fim_provimento ignore nulls) as data_fim_provimento,
        array_agg(distinct setor_sigla ignore nulls) as setor_sigla,
        array_agg(distinct setor ignore nulls) as setor,
        array_agg(distinct cargo ignore nulls) as cargo

    from profissionais_ergon_032025
    group by cpf

),

profissionais_cnes as (
    SELECT 
        profissional__cpf,
        profissional__cns,
        profissional__nome,
        estabelecimento__id_cnes,
        estabelecimento__nome_fantasia,
        profissional__cbo
    FROM `rj-sms.projeto_cnes_subgeral.profissionais_mrj_sus`
    where 
        metadado__ano_competencia = 2025
        and metadado__mes_competencia = 4
),

cnes_agrupado as (
    select
        profissional__cpf,
        array_agg(distinct profissional__nome ignore nulls) as profissional_nome_cnes,
        array_agg(distinct estabelecimento__id_cnes ignore nulls) as estabelecimento_id_cnes,
        array_agg(distinct estabelecimento__nome_fantasia ignore nulls) as estabelecimento_nome_fantasia,
        array_agg(distinct profissional__cbo ignore nulls) as profissional_cbo

    from profissionais_cnes
    group by profissional__cpf
)

select
    coalesce(cnes.profissional__cpf, ergon.cpf) as profissional_cpf,
    cnes.profissional_nome_cnes,
    ergon.profissional_nome as profissional_nome_ergon,
    estabelecimento_id_cnes,
    estabelecimento_nome_fantasia as estabelecimento_nome_fantasia_cnes,
    setor_sigla as setor_sigla_ergon,
    setor as setor_ergon,
    profissional_cbo as profissionais_ocupacao_cnes,
    cargo as profissionais_ocupacao_ergon,
    case
        when cnes.profissional__cpf is not null and ergon.cpf is not null then true
        else false
    end as houve_match

from cnes_agrupado as cnes 
full outer join ergon_agrupado as ergon
on safe_cast(cnes.profissional__cpf as int64) = safe_cast(ergon.cpf as int64)