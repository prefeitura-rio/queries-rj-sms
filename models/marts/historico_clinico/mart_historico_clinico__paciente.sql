{{
    config(
        alias="paciente",
        schema="saude_historico_clinico",
        tag=["hci", "paciente"],
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

-- This code integrates patient data from three sources:
-- rj-sms.brutos_prontuario_vitacare.paciente (vitacare)
-- rj-sms.brutos_plataforma_vitai.paciente (vitai)
-- rj-sms.brutos_plataforma_smsrio.paciente (smsrio)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.
-- dbt run --select int_historico_clinico__paciente__vitacare
-- int_historico_clinico__paciente__smsrio int_historico_clinico__paciente__vitai
-- mart_historico_clinico__paciente
-- mart_historico_clinico__paciente_suspeitos
-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";
-- vitacare: Patient base table
with
    vitacare_tb as (
        select
            cpf,
            cns,
            dados.nome,
            dados.cpf_valido_indicador,
            dados.nome_social,
            dados.data_nascimento,
            dados.genero,
            dados.raca,
            dados.obito_indicador,
            dados.obito_data,
            dados.mae_nome,
            dados.pai_nome,
            dados.metadados,
            equipe_saude_familia,
            contato,
            endereco,
            prontuario
        from
            {{ ref("int_historico_clinico__paciente__vitacare") }},
            unnest(dados) as dados
        where dados.rank = 1
    -- AND cpf = cpf_filter
    ),
    -- vitai: Deceased base table
    base_obitos_vitai as (
        select * from {{ ref("int_historico_clinico__obito__vitai") }}
    ),

    -- vitai: Patient base table
    vitai_tb as (
        select
            cpf,
            cns,
            dados.nome,
            dados.cpf_valido_indicador,
            dados.nome_social,
            dados.data_nascimento,
            dados.genero,
            dados.raca,
            dados.obito_indicador,
            dados.obito_data,
            dados.mae_nome,
            dados.pai_nome,
            dados.metadados,
            contato,
            endereco,
            prontuario
        from {{ ref("int_historico_clinico__paciente__vitai") }}, unnest(dados) as dados
        where dados.rank = 1
    -- AND cpf = cpf_filter
    ),

    -- smsrio: Patient base table
    smsrio_tb as (
        select
            cpf,
            cns,
            dados.nome,
            dados.cpf_valido_indicador,
            dados.nome_social,
            dados.data_nascimento,
            dados.genero,
            dados.raca,
            dados.obito_indicador,
            dados.obito_data,
            dados.mae_nome,
            dados.pai_nome,
            dados.metadados,
            contato,
            endereco,
            prontuario
        from
            {{ ref("int_historico_clinico__paciente__smsrio") }}, unnest(dados) as dados
        where dados.rank = 1
    -- AND cpf = cpf_filter
    ),

    -- Paciente Dados: Merges patient data
    all_cpfs as (
        select distinct cpf
        from
            (
                select cpf
                from vitacare_tb
                union all
                select cpf
                from vitai_tb
                union all
                select cpf
                from smsrio_tb
            )
    ),

    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- Merge data from different sources
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- CNS Dados: Merges CNS data, grouping by patient 
    -- UNION 1. Vitacare | 2. Vitai | 3. smsrio
    cns_dedup as (
        select
            cpf,
            cns,
            row_number() over (
                partition by cpf order by merge_order asc, rank asc
            ) as rank,
            merge_order,
            sistema
        from
            (
                select
                    cpf,
                    cns,
                    rank,
                    merge_order,
                    row_number() over (
                        partition by cpf, cns order by merge_order, rank asc
                    ) as dedup_rank,
                    sistema
                from
                    (
                        select
                            cpf,
                            cns.cns as cns,
                            cns.rank as rank,
                            "vitacare" as sistema,
                            1 as merge_order
                        from vitacare_tb, unnest(cns) as cns
                        where cns.cns_valido_indicador is true
                        union all
                        select
                            cpf,
                            cns.cns as cns,
                            cns.rank as rank,
                            "vitai" as sistema,
                            2 as merge_order
                        from vitai_tb, unnest(cns) as cns
                        where cns.cns_valido_indicador is true
                        union all
                        select
                            cpf,
                            cns.cns as cns,
                            cns.rank as rank,
                            "smsrio" as sistema,
                            3 as merge_order
                        from smsrio_tb, unnest(cns) as cns
                        where cns.cns_valido_indicador is true
                    )
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
        order by merge_order asc, rank asc
    ),
    cns_contagem as (
        select cpf, case when cc.cpf_count > 1 then null else cd.cns end as cns
        from cns_dedup cd
        left join
            (
                select cns, count(distinct cpf) as cpf_count from cns_dedup group by cns
            ) as cc
            on cd.cns = cc.cns
        order by merge_order asc, rank asc
    ),

    cns_dados as (
        select cpf, array_agg(cns) as cns
        from cns_contagem
        where cns is not null
        group by cpf
    ),

    -- Equipe Saude Familia Dados: Groups family health team data by patient.
    -- ONLY vitacare
    equipe_saude_familia_dados as (select cpf, equipe_saude_familia from vitacare_tb),

    -- Contato Dados: Merges contact data 
    -- UNION: 1. Vitacare | 2. smsrio | 3. Vitai
    telefone_dedup as (
        select
            cpf,
            ddd,
            valor,
            row_number() over (
                partition by cpf order by merge_order asc, rank asc
            ) as rank,
            sistema
        from
            (
                select
                    cpf,
                    ddd,
                    valor,
                    rank,
                    merge_order,
                    row_number() over (
                        partition by cpf, valor order by merge_order, rank asc
                    ) as dedup_rank,
                    sistema
                from
                    (
                        select
                            cpf,
                            telefone.ddd,
                            telefone.valor,
                            telefone.rank,
                            "vitacare" as sistema,
                            1 as merge_order
                        from vitacare_tb, unnest(contato.telefone) as telefone  -- Expandindo os elementos da array struct de telefone
                        union all
                        select
                            cpf,
                            telefone.ddd,
                            telefone.valor,
                            telefone.rank,
                            "smsrio" as sistema,
                            2 as merge_order
                        from smsrio_tb, unnest(contato.telefone) as telefone
                        union all
                        select
                            cpf,
                            telefone.ddd,
                            telefone.valor,
                            telefone.rank,
                            "vitai" as sistema,
                            3 as merge_order
                        from vitai_tb, unnest(contato.telefone) as telefone
                    )
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
        order by merge_order asc, rank asc
    ),

    email_dedup as (
        select
            cpf,
            valor,
            row_number() over (
                partition by cpf order by merge_order asc, rank asc
            ) as rank,
            sistema
        from
            (
                select
                    cpf,
                    valor,
                    rank,
                    merge_order,
                    row_number() over (
                        partition by cpf, valor order by merge_order, rank asc
                    ) as dedup_rank,
                    sistema
                from
                    (
                        select
                            cpf,
                            email.valor,
                            email.rank,
                            "vitacare" as sistema,
                            1 as merge_order
                        from vitacare_tb, unnest(contato.email) as email  -- Expandindo os elementos da array struct de email
                        union all
                        select
                            cpf,
                            email.valor,
                            email.rank,
                            "smsrio" as sistema,
                            2 as merge_order
                        from smsrio_tb, unnest(contato.email) as email
                        union all
                        select
                            cpf,
                            email.valor,
                            email.rank,
                            "vitai" as sistema,
                            3 as merge_order
                        from vitai_tb, unnest(contato.email) as email
                    )
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
        order by merge_order asc, rank asc
    ),

    contato_dados as (
        select
            a.cpf as cpf,
            struct(
                array_agg(struct(t.ddd, t.valor, t.sistema, t.rank)) as telefone,
                array_agg(struct(e.valor, e.sistema, e.rank)) as email
            ) as contato
        from all_cpfs a
        left join telefone_dedup t on a.cpf = t.cpf
        left join email_dedup e on a.cpf = e.cpf
        group by a.cpf
    ),

    -- Endereco Dados: Merges address information
    -- UNION: 1. Vitacare | 2. smsrio | 3. Vitai
    endereco_dedup as (
        select
            cpf,
            cep,
            tipo_logradouro,
            logradouro,
            numero,
            complemento,
            bairro,
            cidade,
            estado,
            datahora_ultima_atualizacao,
            row_number() over (
                partition by cpf order by merge_order asc, rank asc
            ) as rank,
            sistema
        from
            (
                select
                    cpf,
                    cep,
                    tipo_logradouro,
                    logradouro,
                    numero,
                    complemento,
                    bairro,
                    cidade,
                    estado,
                    datahora_ultima_atualizacao,
                    merge_order,
                    rank,
                    row_number() over (
                        partition by cpf, datahora_ultima_atualizacao
                        order by merge_order, rank asc
                    ) as dedup_rank,
                    sistema
                from
                    (
                        select
                            cpf,
                            endereco.cep,
                            endereco.tipo_logradouro,
                            endereco.logradouro,
                            endereco.numero,
                            endereco.complemento,
                            endereco.bairro,
                            endereco.cidade,
                            endereco.estado,
                            endereco.datahora_ultima_atualizacao,
                            endereco.rank,
                            "vitacare" as sistema,
                            1 as merge_order
                        from vitacare_tb, unnest(endereco) as endereco  -- Expandindo os elementos da array struct de endereço
                        union all
                        select
                            cpf,
                            endereco.cep,
                            endereco.tipo_logradouro,
                            endereco.logradouro,
                            endereco.numero,
                            endereco.complemento,
                            endereco.bairro,
                            endereco.cidade,
                            endereco.estado,
                            endereco.datahora_ultima_atualizacao,
                            endereco.rank,
                            "smsrio" as sistema,
                            2 as merge_order
                        from smsrio_tb, unnest(endereco) as endereco
                        union all
                        select
                            cpf,
                            endereco.cep,
                            endereco.tipo_logradouro,
                            endereco.logradouro,
                            endereco.numero,
                            endereco.complemento,
                            endereco.bairro,
                            endereco.cidade,
                            endereco.estado,
                            endereco.datahora_ultima_atualizacao,
                            endereco.rank,
                            "vitai" as sistema,
                            3 as merge_order
                        from vitai_tb, unnest(endereco) as endereco
                    )
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
    -- ORDER BY merge_order ASC, rank ASC
    ),

    endereco_dados as (
        select
            cpf,
            array_agg(
                struct(
                    cep,
                    tipo_logradouro,
                    logradouro,
                    numero,
                    complemento,
                    bairro,
                    cidade,
                    estado,
                    datahora_ultima_atualizacao,
                    sistema,
                    rank
                )
            ) as endereco
        from endereco_dedup
        group by cpf
    ),

    -- Prontuario Dados: Merges system medical record data
    -- UNION: 1. Vitacare | 2. smsrio | 3. Vitai
    prontuario_dedup as (
        select
            cpf,
            sistema,
            id_cnes,
            id_paciente,
            row_number() over (
                partition by cpf order by merge_order asc, rank asc
            ) as rank
        from
            (
                select
                    cpf,
                    sistema,
                    id_cnes,
                    id_paciente,
                    rank,
                    merge_order,
                    row_number() over (
                        partition by cpf, id_cnes, id_paciente
                        order by merge_order, rank asc
                    ) as dedup_rank
                from
                    (
                        select
                            vc.cpf,
                            "vitacare" as sistema,
                            prontuario.id_cnes,
                            prontuario.id_paciente,
                            prontuario.rank,
                            1 as merge_order
                        from vitacare_tb vc, unnest(prontuario) as prontuario
                        union all
                        select
                            sm.cpf,
                            "smsrio" as sistema,
                            prontuario.id_cnes,
                            prontuario.id_paciente,
                            prontuario.rank,
                            2 as merge_order
                        from smsrio_tb sm, unnest(prontuario) as prontuario
                        union all
                        select
                            vi.cpf,
                            "vitai" as sistema,
                            prontuario.id_cnes,
                            prontuario.id_paciente,
                            prontuario.rank,
                            3 as merge_order
                        from vitai_tb vi, unnest(prontuario) as prontuario
                    )
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
        order by merge_order asc, rank asc
    ),

    prontuario_dados as (
        select cpf, array_agg(struct(sistema, id_cnes, id_paciente, rank)) as prontuario
        from prontuario_dedup
        group by cpf
    ),

    -- merge priority:
    -- nome:             1. smsrio   | 2. Vitacare  | 3. Vitai
    -- nome_social:      1. Vitai    
    -- data_nascimento:  1. smsrio   | 2. Vitacare  | 3. Vitai
    -- genero:           1. Vitacare | 2. smsrio    | 3. Vitai
    -- raca:             1. Vitacare | 2. smsrio    | 3. Vitai
    -- obito_indicador:  1. Vitacare | 2. smsrio    | 3. Vitai
    -- obito_data:       1. Vitacare | 2. smsrio    | 3. Vitai
    -- mae_nome:         1. smsrio   | 2. Vitacare  | 3. Vitai
    -- pai_nome:         1. smsrio   | 2. Vitacare  | 3. Vitai
    paciente_dados as (
        select
            cpfs.cpf,
            struct(
                case
                    when sm.cpf is not null
                    then sm.nome
                    when vc.cpf is not null
                    then vc.nome
                    when vi.cpf is not null
                    then vi.nome
                    else null
                end as nome,
                case
                    when vc.cpf is not null
                    then vc.nome_social
                    -- WHEN sm.cpf THEN sm.nome_social  -- smsrio não possui nome social
                    -- WHEN vi.cpf IS NOT NULL THEN vi.nome_social  -- vitai não
                    -- possui nome social
                    else null
                end as nome_social,
                case
                    when sm.cpf is not null
                    then sm.data_nascimento
                    when vc.cpf is not null
                    then vc.data_nascimento
                    when vi.cpf is not null
                    then vi.data_nascimento
                    else null
                end as data_nascimento,
                coalesce(vc.genero, sm.genero, vi.genero) as genero,
                coalesce(vc.raca, sm.raca, vi.raca) as raca,
                case
                    when
                        (
                            (
                                coalesce(
                                    vc.obito_indicador,
                                    sm.obito_indicador,
                                    vi.obito_indicador
                                )
                                is false
                                or coalesce(
                                    vc.obito_indicador,
                                    sm.obito_indicador,
                                    vi.obito_indicador
                                )
                                is null
                            )
                        )
                        and (base_obitos_vitai.cpf is not null)
                    then true
                    else
                        coalesce(
                            vc.obito_indicador, sm.obito_indicador, vi.obito_indicador
                        )
                end as obito_indicador,
                case
                    when
                        coalesce(vc.obito_data, sm.obito_data, vi.obito_data) is null
                        and (base_obitos_vitai.obito_data is not null)
                    then base_obitos_vitai.obito_data
                    else coalesce(vc.obito_data, sm.obito_data, vi.obito_data)
                end as obito_data,
                case
                    when sm.cpf is not null
                    then sm.mae_nome
                    when vc.cpf is not null
                    then vc.mae_nome
                    when vi.cpf is not null
                    then vi.mae_nome
                    else null
                end as mae_nome,
                case
                    when sm.cpf is not null
                    then sm.pai_nome
                    when vc.cpf is not null
                    then vc.pai_nome
                    when vi.cpf is not null
                    then vi.pai_nome
                    else null
                end as pai_nome,
                case
                    when sm.cpf is not null then true else false
                end as identidade_validada_indicador,
                case
                    when sm.cpf is not null
                    then sm.cpf_valido_indicador
                    when vc.cpf is not null
                    then vc.cpf_valido_indicador
                    when vi.cpf is not null
                    then vi.cpf_valido_indicador
                    else null
                end as cpf_valido_indicador
            ) as dados
        from all_cpfs cpfs
        left join vitacare_tb vc on cpfs.cpf = vc.cpf
        left join vitai_tb vi on cpfs.cpf = vi.cpf
        left join smsrio_tb sm on cpfs.cpf = sm.cpf
        left join base_obitos_vitai on cpfs.cpf = base_obitos_vitai.cpf
    ),

    -- -- FINAL JOIN: Joins all the data previously processed, creating the
    -- -- integrated table of the patients.
    paciente_integrado as (
        select
            pd.cpf,
            cns.cns,
            pd.dados,
            esf.equipe_saude_familia,
            ct.contato,
            ed.endereco,
            pt.prontuario,
            struct(current_timestamp() as processed_at) as metadados,
            safe_cast(pd.cpf as int64) as cpf_particao
        from paciente_dados pd
        left join cns_dados cns on pd.cpf = cns.cpf
        left join equipe_saude_familia_dados esf on pd.cpf = esf.cpf
        left join contato_dados ct on pd.cpf = ct.cpf
        left join endereco_dados ed on pd.cpf = ed.cpf
        left join prontuario_dados pt on pd.cpf = pt.cpf
        where
            pd.dados.nome is not null
            -- AND pd.dados.data_nascimento IS NOT NULL
            and pd.dados.cpf_valido_indicador is true

    )

select *
from paciente_integrado
