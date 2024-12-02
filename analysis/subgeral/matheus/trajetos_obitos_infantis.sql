with
    -- Seleciona os dados brutos de óbitos infantis, garantindo que o campo `data_nasc` seja convertido para o formato de data.
    obitos_raw as (
        select
            nome as nome,
            nome_mae as nome_mae,
            nome_pai as nome_pai,
            safe_cast(data_nasc as date) as data_nasc
        from `rj-sms-sandbox.sub_geral.obitos_infantis2324`
    ),

    -- Filtra os dados de pacientes históricos do sistema HCI, normalizando os nomes para minúsculas
    -- e convertendo `data_nascimento` para o formato de data. Limita os pacientes nascidos após 2020.
    pacientes_hci as (
        select
            cpf,
            cns,
            lower(dados.nome) as nome,
            lower(dados.mae_nome) as nome_mae,
            lower(dados.pai_nome) as nome_pai,
            safe_cast(dados.data_nascimento as date) as data_nasc
        from `rj-sms.saude_historico_clinico.paciente`
        where extract(year from dados.data_nascimento) > 2020
    ),

    -- Identifica os óbitos infantis ao cruzar os dados de óbitos com os pacientes históricos,
    -- considerando o nome, data de nascimento e pelo menos um dos pais (mãe ou pai). Filtra registros com CPF ou CNS válido.
    obitos_identificados as (
        select distinct p.cpf, p.cns, o.nome, o.data_nasc, o.nome_mae, o.nome_pai
        from obitos_raw o
        inner join
            pacientes_hci p
            on o.nome = p.nome
            and o.data_nasc = p.data_nasc
            and (o.nome_mae = p.nome_mae or o.nome_pai = p.nome_pai)
        where p.cpf is not null or array_length(p.cns) > 0
    ),

    -- Desagrega os CNS (cartões do SUS) dos óbitos identificados, criando uma linha por CNS. 
    obitos_desagg_cns as (
        select cpf, cns_element as cns
        from obitos_identificados, unnest(cns) as cns_element
    ),

    -- Desagrega os CNS dos episódios assistenciais, criando uma linha por CNS associado a cada episódio.
    episodios_assistenciais_desagg_cns as (
        select
            e.id_episodio,
            e.paciente.cpf as cpf,
            cns_element as cns,
            e.entrada_datahora,
            e.estabelecimento.id_cnes,
            e.estabelecimento.nome as estabelecimento_nome,
            e.tipo,
            e.subtipo
        from `rj-sms.saude_historico_clinico.episodio_assistencial` e
        left join unnest(e.paciente.cns) as cns_element
    ),

    -- Filtra os episódios assistenciais relevantes que têm ligação com os óbitos identificados,
    -- seja pelo CPF ou pelo CNS.
    episodios_assistenciais_alvo as (
        select distinct
            ea.id_episodio,
            ea.cpf,
            ea.cns,
            ea.entrada_datahora,
            ea.id_cnes,
            ea.estabelecimento_nome,
            ea.tipo,
            ea.subtipo
        from episodios_assistenciais_desagg_cns ea
        join
            obitos_desagg_cns oi
            on (ea.cpf is not null and ea.cpf = oi.cpf)
            or (ea.cns is not null and ea.cns = oi.cns)
    ),

    -- Consolida os dados finais, agrupando os episódios assistenciais por CPF.
    -- Agrega informações como IDs dos episódios, datas de entrada, estabelecimentos, tipos e subtipo de atendimento,
    -- e conta a quantidade de episódios distintos.
    final as (
        select
            me.cpf,
            array_agg(distinct me.id_episodio) as episodios_ids,
            array_agg(
                me.entrada_datahora order by me.entrada_datahora
            ) as entrada_datas,
            array_agg(distinct me.estabelecimento_nome) as estabelecimentos,
            array_agg(distinct me.id_cnes) as id_cnes,
            array_agg(distinct me.tipo) as tipos,
            array_agg(distinct me.subtipo) as subtipos,
            count(distinct me.id_episodio) as qtd_episodios_assistenciais
        from episodios_assistenciais_alvo me
        group by me.cpf
        order by qtd_episodios_assistenciais desc
    )

-- Seleciona todos os dados consolidados para exibição.
select *
from final