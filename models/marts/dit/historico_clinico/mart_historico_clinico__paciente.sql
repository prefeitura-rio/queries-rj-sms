{{
    config(
        alias="paciente",
        schema="saude_historico_clinico",
        tags=["hci", "paciente"],
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

    pcsm_unidades as (
        select 
            u.id_unidade_saude as id_unidade,
            coalesce(e.id_cnes,u.codigo_nacional_estabelecimento_saude) as cnes,
            coalesce(e.nome_acentuado,u.nome_unidade_saude) as nome_unidade
        from {{ ref("raw_pcsm_unidades_saude") }} as u
        left join {{ ref("dim_estabelecimento") }} as e
            on e.id_cnes = u.codigo_nacional_estabelecimento_saude
    ),

    pcsm_pacientes as (
        SELECT 
            paciente.numero_cpf_paciente as cpf,
            paciente.id_paciente as id_pcsm,
            paciente.descricao_status_acompanhamento as status_acompanhamento,
            paciente.id_unidade_caps_referencia as id_caps
        FROM {{ ref("raw_pcsm_pacientes") }} as paciente 
        QUALIFY row_number() over (partition by paciente.numero_cpf_paciente order by paciente.loaded_at desc ) = 1
    ),

    saude_mental as (
        select 
            p.cpf,
            struct(
                p.id_pcsm,
                p.status_acompanhamento,
                u.nome_unidade,
                u.cnes
            ) as saude_mental
        from pcsm_pacientes p 
        left join pcsm_unidades u on p.id_caps = u.id_unidade
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

    contato_telefone_dados as (
        select
            t.cpf,
            array_agg(
                struct(t.ddd, t.valor, lower(t.sistema) as sistema, t.rank)
            ) as telefone,
        from telefone_dedup t
        where t.valor is not null
        group by t.cpf
    ),

    contato_email_dados as (
        select
            e.cpf,
            array_agg(
                struct(lower(e.valor) as valor, lower(e.sistema) as sistema, e.rank)
            ) as email
        from email_dedup e
        where e.valor is not null
        group by e.cpf
    ),

    contato_dados as (
        select
            a.cpf,
            struct(
                contato_telefone_dados.telefone, contato_email_dados.email
            ) as contato
        from all_cpfs a
        left join contato_email_dados using (cpf)
        left join contato_telefone_dados using (cpf)
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
        --order by merge_order asc, rank asc
    ),

    prontuario_dados as (
        select cpf, array_agg(struct(sistema, id_cnes, id_paciente, rank)) as prontuario
        from prontuario_dedup
        group by cpf
    ),

    -- merge priority:
    -- nome:             1. smsrio   | 2. Vitacare  | 3. Vitai
    -- nome_social:      1. Vitacare | 2. Vitai
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
            {{
            dbt_utils.generate_surrogate_key(
                    [
                        "cpfs.cpf",
                    ]
                )
            }} as id_paciente,
            case
                when sm.cpf is not null
                then sm.nome
                when vc.cpf is not null
                then vc.nome
                when vi.cpf is not null
                then vi.nome
                else null
            end as nome,
            coalesce(vc.nome_social, vi.nome_social) as nome_social,
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
        from all_cpfs cpfs
        left join vitacare_tb vc on cpfs.cpf = vc.cpf
        left join vitai_tb vi on cpfs.cpf = vi.cpf
        left join smsrio_tb sm on cpfs.cpf = sm.cpf
        left join base_obitos_vitai on cpfs.cpf = base_obitos_vitai.cpf
    ),

    paciente_dados_nomes_unicos as (
        select
            cpf,
            struct(
                id_paciente,
                nome,
                case
                    -- Só queremos `nome_social` se não for igual a `nome`
                    -- ainda que falte um ou outro sobrenome
                    when starts_with(nome, nome_social)
                    then null
                    when {{ is_same_name("nome", "nome_social") }}
                    then null
                    -- O campo de nome social às vezes é usado como nome da mãe
                    when {{ is_same_name("mae_nome", "nome_social" )}}
                    then null
                    else nome_social
                end as nome_social,
                data_nascimento,
                genero,
                raca,
                obito_indicador,
                obito_data,
                mae_nome,
                pai_nome,
                identidade_validada_indicador,
                cpf_valido_indicador
            ) as dados
        from paciente_dados
    ),

    -- -- FINAL JOIN: Joins all the data previously processed, creating the
    -- -- integrated table of the patients.
    paciente_integrado as (
        select  
            pd.cpf,
            cns.cns,
            pd.dados,
            esf.equipe_saude_familia,
            sm.saude_mental,
            ct.contato,
            ed.endereco,
            pt.prontuario,
            struct(current_timestamp() as processed_at) as metadados,
            safe_cast(pd.cpf as int64) as cpf_particao
        from paciente_dados_nomes_unicos pd
        left join cns_dados cns on pd.cpf = cns.cpf
        left join equipe_saude_familia_dados esf on pd.cpf = esf.cpf
        left join contato_dados ct on pd.cpf = ct.cpf
        left join endereco_dados ed on pd.cpf = ed.cpf
        left join prontuario_dados pt on pd.cpf = pt.cpf
        left join saude_mental sm on sm.cpf = pd.cpf
        where
            pd.dados.nome is not null
            -- AND pd.dados.data_nascimento IS NOT NULL
            and pd.dados.cpf_valido_indicador is true

    )

select *
from paciente_integrado
