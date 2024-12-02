-- identifica pacientes a partir de base de óbitos infantis (2023, 2024)
-- busca episodios assistenciais desses pacientes
with
    obitos_raw as (
        select nome, nome_mae, nome_pai, sexo, data_nasc
        from `rj-sms-sandbox.sub_geral.obitos_infantis2324`
    ),

    pacientes as (
        select
            cpf,
            cns,
            lower(dados.nome) as nome2,
            lower(dados.mae_nome) as nome_mae2,
            lower(dados.pai_nome) as nome_pai2,
            lower(dados.genero) as genero,
            safe_cast(dados.data_nascimento as string) as data_nasc2
        from `rj-sms.saude_historico_clinico.paciente`
        where extract(year from dados.data_nascimento) > 2020
    ),

    identificacao as (
        select *
        from obitos_raw as o
        left join
            pacientes as p
            on o.nome = p.nome2
            and o.data_nasc = p.data_nasc2
            and (o.nome_mae = p.nome_mae2 or o.nome_pai = p.nome_pai2)
    ),

    filtrado as (
        select distinct * from identificacao where cpf is not null or cns is not null
    ),

    obitos_infantis as (
        select cpf, cns, nome, data_nasc, nome_mae, nome_pai from filtrado
    ),

    obitos_infantis_expanded as (
        select cpf, cns_element
        from obitos_infantis, unnest(cns) as cns_element
        where cns_element is not null
    ),

    episodios_assistenciais_expanded as (
        select
            e.paciente.cpf as cpf,
            cns_element,
            e.entrada_datahora,
            e.estabelecimento.id_cnes,
            e.estabelecimento.nome
        from `rj-sms.saude_historico_clinico.episodio_assistencial` as e
        left join unnest(e.paciente.cns) as cns_element
    ),

    matched_episodes as (
        select distinct
            ea.cpf, ea.cns_element as cns, ea.entrada_datahora, ea.id_cnes, ea.nome
        from episodios_assistenciais_expanded as ea
        inner join
            obitos_infantis_expanded as oi
            on (ea.cpf is not null and ea.cpf = oi.cpf)
            or (ea.cns_element is not null and ea.cns_element = oi.cns_element)
    ),

    dedupl as (select distinct * from matched_episodes),

    final as (
        select
            cpf,
            array_agg(
                entrada_datahora ignore nulls order by entrada_datahora
            ) as entrada_datahora,
            array_agg(nome ignore nulls order by entrada_datahora) as estabelecimento,
            array_agg(id_cnes ignore nulls order by entrada_datahora) as id_cnes
        from dedupl
        group by cpf
    )

select *
from final
