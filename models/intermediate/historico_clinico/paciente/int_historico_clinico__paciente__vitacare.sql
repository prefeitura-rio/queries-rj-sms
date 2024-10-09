{{
    config(
        alias="paciente_vitacare",
        schema="intermediario_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

-- This code integrates patient data from vitacare:
-- rj-sms.brutos_prontuario_vitacare.paciente (vitacare)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.
-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- Get source data and standardize
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- Patient base table
with
    paciente as (
        select
            *,
            row_number() over (
                partition by cpf
                order by
                    equipe_familia_indicador desc,
                    data_atualizacao_vinculo_equipe desc,
                    data_ultima_atualizacao_cadastral desc,
                    cadastro_permanente_indicador desc
            ) as rank
        from {{ ref("raw_prontuario_vitacare__paciente") }}
        where {{ validate_cpf("cpf") }}

    ),

    paciente_com_cadastro_permanente as (
        select * from paciente where cadastro_permanente_indicador = true
    ),

    paciente_com_equipe as (
        select *
        from paciente_com_cadastro_permanente
        where equipe_familia_indicador = true
    ),

    -- CNS
    cns_ranked as (
        select
            cpf,
            cns,
            row_number() over (
                partition by cpf, cns
                order by
                    data_atualizacao_vinculo_equipe desc,
                    cadastro_permanente_indicador desc,
                    updated_at desc
            ) as rank,
        from
            (
                select
                    cpf,
                    case when trim(cns) in ('NONE') then null else trim(cns) end as cns,
                    data_atualizacao_vinculo_equipe,
                    cadastro_permanente_indicador,
                    updated_at
                from paciente
            )
        where cns is not null
        group by
            cpf,
            cns,
            data_atualizacao_vinculo_equipe,
            cadastro_permanente_indicador,
            updated_at
    ),

    -- CNS Dados
    cns_dedup as (
        select
            cpf,
            cns,
            row_number() over (
                partition by cpf order by merge_order asc, rank asc
            ) as rank
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
                from (select cpf, cns, rank, 1 as merge_order from cns_ranked)
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
        order by merge_order asc, rank asc
    ),

cns_validated as (
    select
        cns,
        {{validate_cns('cns')}} as cns_valido_indicador,
    from (
        select distinct cns from cns_dedup
    )
),

cns_dados as (
    select 
        cpf,
        array_agg(
                struct(
                    cd.cns, 
                    cv.cns_valido_indicador,
                    cd.rank
                )
        ) as cns
    from cns_dedup cd
    join cns_validated cv
        on cd.cns = cv.cns
    group by cpf
),

    -- EQUIPE SAUDE FAMILIA vitacare: Extracts and ranks family health teams
    -- clinica da familia
    source_clinica_familia as (
        select *
        from paciente_com_cadastro_permanente
        qualify
            row_number() over (
                partition by cpf, id_cnes
                order by
                    data_atualizacao_vinculo_equipe desc,
                    data_ultima_atualizacao_cadastral desc,
                    updated_at desc
            )
            = 1
    ),

    dim_clinica_familia as (
        select
            cf.cpf,
            cf.id_cnes,
            {{ proper_estabelecimento("e.nome_limpo") }} as nome,
            if(array_length(e.telefone) > 0, e.telefone[offset(0)], null) as telefone,
            cf.data_atualizacao_vinculo_equipe,
            row_number() over (
                partition by cf.cpf
                order by
                    cf.data_atualizacao_vinculo_equipe desc,
                    cf.data_ultima_atualizacao_cadastral desc
            ) as rank,
        from source_clinica_familia as cf
        join {{ ref("dim_estabelecimento") }} e on cf.id_cnes = e.id_cnes
    ),

    -- medicos data
    medicos_data as (
        select
            e.id_ine,
            array_agg(
                struct(p.id_profissional_sus, {{ proper_br("p.nome") }} as nome)
            ) as medicos
        from {{ ref("dim_equipe") }} e
        left join unnest(e.medicos) as medico_id
        left join
            {{ ref("dim_profissional_saude") }} p on medico_id = p.id_profissional_sus
        group by e.id_ine
    ),

    -- enfermeiros data
    enfermeiros_data as (
        select
            e.id_ine,
            array_agg(
                struct(p.id_profissional_sus, {{ proper_br("p.nome") }} as nome)
            ) as enfermeiros
        from {{ ref("dim_equipe") }} e  -- `rj-sms-dev`.`saude_dados_mestres`.`equipe_profissional_saude` 
        left join unnest(e.enfermeiros) as enfermeiro_id
        left join
            {{ ref("dim_profissional_saude") }} p  -- `rj-sms-dev`.`saude_dados_mestres`.`profissional_saude`
            on enfermeiro_id = p.id_profissional_sus
        group by e.id_ine
    ),

    source_equipe_familia as (
        select *
        from paciente_com_equipe
        qualify
            row_number() over (
                partition by cpf, id_cnes
                order by
                    data_atualizacao_vinculo_equipe desc,
                    data_ultima_atualizacao_cadastral desc,
                    updated_at desc
            )
            = 1
    ),

    equipe_saude_familia_enriquecida as (
        select
            f.cpf,
            f.id_ine,
            {{ proper_br("e.nome_referencia") }} as nome,
            e.telefone,
            m.medicos,
            en.enfermeiros,
            f.data_atualizacao_vinculo_equipe as datahora_ultima_atualizacao,
            row_number() over (
                partition by f.cpf
                order by
                    f.data_atualizacao_vinculo_equipe desc,
                    data_ultima_atualizacao_cadastral desc
            ) as rank
        from source_equipe_familia as f
        join {{ ref("dim_equipe") }} e on f.id_ine = e.id_ine
        left join medicos_data m on f.id_ine = m.id_ine
        left join enfermeiros_data en on f.id_ine = en.id_ine
    ),

    dim_equipe_familia as (
        select
            ef.cpf,
            array_agg(
                struct(
                    ef.id_ine,
                    ef.nome,
                    ef.telefone,
                    ef.medicos,
                    ef.enfermeiros,
                    struct(cf.id_cnes, cf.nome, cf.telefone) as clinica_familia,
                    ef.datahora_ultima_atualizacao,
                    ef.rank
                )
            ) as equipe_saude_familia
        from equipe_saude_familia_enriquecida ef
        left join dim_clinica_familia cf on ef.cpf = cf.cpf
        group by cpf
    ),

    -- CONTATO TELEPHONE
    vitacare_contato_telefone as (
        select
            cpf,
            tipo,
            valor_original,
            case
                when length(valor) in (10, 11)
                then substr(valor, 1, 2)  -- Keep only the first 2 digits (DDD)
                else null
            end as ddd,
            case
                when length(valor) in (8, 9)
                then valor  -- For numbers with 8 or 9 digits, keep the original value
                when length(valor) = 10
                then substr(valor, 3, 8)  -- Keep only the last 8 digits (discard the first 2)
                when length(valor) = 11
                then substr(valor, 3, 9)  -- Keep only the last 9 digits (discard the first 2)
                else null
            end as valor,
            case
                when length(valor) = 8
                then 'fixo'
                when length(valor) = 9
                then 'celular'
                when length(valor) = 10
                then 'ddd_fixo'
                when length(valor) = 11
                then 'ddd_celular'
                else null
            end as valor_tipo,
            length(valor) as len,
            rank
        from
            (
                select
                    cpf,
                    'telefone' as tipo,
                    telefone as valor_original,
                    {{ padronize_telefone("telefone") }} as valor,
                    row_number() over (
                        partition by cpf
                        order by
                            data_atualizacao_vinculo_equipe desc,
                            cadastro_permanente_indicador desc,
                            updated_at desc
                    ) as rank
                from paciente
                group by
                    cpf,
                    telefone,
                    data_atualizacao_vinculo_equipe,
                    cadastro_permanente_indicador,
                    updated_at
            )
        where not (trim(valor) in ("()", "") and (rank >= 2))
    ),

    -- CONTATO EMAIL
    vitacare_contato_email as (
        select
            cpf,
            tipo,
            case when trim(valor) in ("()", "") then null else valor end as valor,
            rank
        from
            (
                select
                    cpf,
                    'email' as tipo,
                    email as valor,
                    row_number() over (
                        partition by cpf
                        order by
                            data_atualizacao_vinculo_equipe desc,
                            cadastro_permanente_indicador desc,
                            updated_at desc
                    ) as rank
                from paciente
                group by
                    cpf,
                    email,
                    data_atualizacao_vinculo_equipe,
                    cadastro_permanente_indicador,
                    updated_at
            )
        where not (trim(valor) in ("()", "") and (rank >= 2))
    ),

    telefone_dedup as (
        select
            cpf,
            valor_original,
            ddd,
            valor,
            valor_tipo,
            row_number() over (
                partition by cpf order by merge_order asc, rank asc
            ) as rank,
            sistema
        from
            (
                select
                    cpf,
                    valor_original,
                    ddd,
                    valor,
                    valor_tipo,
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
                            valor_original,
                            ddd,
                            valor,
                            valor_tipo,
                            rank,
                            "vitacare" as sistema,
                            1 as merge_order
                        from vitacare_contato_telefone
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
                        select cpf, valor, rank, "vitacare" as sistema, 1 as merge_order
                        from vitacare_contato_email
                    )
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
        order by merge_order asc, rank asc
    ),

    contato_dados as (
        select
            coalesce(t.cpf, e.cpf) as cpf,
            struct(
                array_agg(
                    struct(
                        t.valor_original,
                        t.ddd,
                        t.valor,
                        t.valor_tipo,
                        lower(t.sistema) as sistema,
                        t.rank
                    )
                ) as telefone,
                array_agg(
                    struct(lower(e.valor) as valor, lower(e.sistema) as sistema, e.rank)
                ) as email
            ) as contato
        from telefone_dedup t
        full outer join email_dedup e on t.cpf = e.cpf
        group by coalesce(t.cpf, e.cpf)
    ),

    -- ENDEREÇO
    vitacare_endereco as (
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
            cast(
                data_atualizacao_vinculo_equipe as string
            ) as datahora_ultima_atualizacao,
            row_number() over (
                partition by cpf
                order by
                    data_atualizacao_vinculo_equipe desc,
                    cadastro_permanente_indicador desc,
                    updated_at desc
            ) as rank
        from paciente
        where logradouro is not null
        group by
            cpf,
            cep,
            tipo_logradouro,
            logradouro,
            numero,
            complemento,
            bairro,
            cidade,
            estado,
            data_atualizacao_vinculo_equipe,
            cadastro_permanente_indicador,
            updated_at
    ),

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
                            cep,
                            tipo_logradouro,
                            logradouro,
                            numero,
                            complemento,
                            bairro,
                            cidade,
                            estado,
                            datahora_ultima_atualizacao,
                            rank,
                            "vitacare" as sistema,
                            1 as merge_order
                        from vitacare_endereco
                    )
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
        order by merge_order asc, rank asc
    ),

    endereco_dados as (
        select
            cpf,
            array_agg(
                struct(
                    cep,
                    lower(tipo_logradouro) as tipo_logradouro,
                    {{ proper_br("logradouro") }} as logradouro,
                    numero,
                    lower(complemento) as complemento,
                    {{ proper_br("bairro") }} as bairro,
                    {{ proper_br("cidade") }} as cidade,
                    lower(estado) as estado,
                    timestamp(
                        datahora_ultima_atualizacao
                    ) as datahora_ultima_atualizacao,
                    lower(sistema) as sistema,
                    rank
                )
            ) as endereco
        from endereco_dedup
        group by cpf
    ),

    -- PRONTUARIO
    vitacare_prontuario as (
        select
            cpf,
            'vitacare' as sistema,
            id_cnes,
            id_paciente,
            row_number() over (
                partition by cpf
                order by
                    data_atualizacao_vinculo_equipe desc,
                    cadastro_permanente_indicador desc,
                    updated_at desc
            ) as rank
        from paciente
        group by
            cpf,
            id_cnes,
            id_paciente,
            data_atualizacao_vinculo_equipe,
            cadastro_permanente_indicador,
            updated_at
    ),

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
                            vi.cpf,
                            "vitacare" as sistema,
                            id_cnes,
                            id_paciente,
                            rank,
                            1 as merge_order
                        from vitacare_prontuario vi
                    )
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
        order by merge_order asc, rank asc
    ),

    prontuario_dados as (
        select
            cpf,
            array_agg(
                struct(lower(sistema) as sistema, id_cnes, id_paciente, rank)
            ) as prontuario
        from prontuario_dedup
        group by cpf
    ),

    -- PACIENTE DADOS
    paciente_metadados as (
        select
            cpf,
            struct(
                -- count the distinct values for each field
                count(distinct nome) as qtd_nomes,
                count(distinct nome_social) as qtd_nomes_sociais,
                count(distinct data_nascimento) as qtd_datas_nascimento,
                count(distinct genero) as qtd_generos,
                count(distinct raca) as qtd_racas,
                count(distinct obito_indicador) as qtd_obitos_indicadores,
                count(distinct obito_data) as qtd_datas_obitos,
                count(distinct mae_nome) as qtd_maes_nomes,
                count(distinct pai_nome) as qtd_pais_nomes,
                count(distinct cpf_valido_indicador) as qtd_cpfs_validos,
                "vitacare" as sistema
            ) as metadados
        from paciente
        group by cpf
    ),

    paciente_dados as (
        select
            pc.cpf,
            array_agg(
                struct(
                    cpf_valido_indicador,
                    {{ proper_br("nome") }} as nome,
                    {{ proper_br("nome_social") }} as nome_social,
                    data_nascimento,
                    lower(genero) as genero,
                    lower(raca) as raca,
                    obito_indicador,
                    obito_data,
                    {{ proper_br("mae_nome") }} as mae_nome,
                    {{ proper_br("pai_nome") }} as pai_nome,
                    rank,
                    pm.metadados
                )
            ) as dados
        from paciente pc
        join paciente_metadados as pm on pc.cpf = pm.cpf
        group by cpf
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
            struct(current_timestamp() as created_at) as metadados,
            cast(pd.cpf as int64) as cpf_particao
        from paciente_dados pd
        left join cns_dados cns on pd.cpf = cns.cpf
        left join dim_equipe_familia esf on pd.cpf = esf.cpf
        left join contato_dados ct on pd.cpf = ct.cpf
        left join endereco_dados ed on pd.cpf = ed.cpf
        left join prontuario_dados pt on pd.cpf = pt.cpf
    )

select *
from paciente_integrado
