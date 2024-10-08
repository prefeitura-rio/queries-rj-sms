{{
    config(
        alias="paciente_vitai",
        materialized="table",
        schema="intermediario_historico_clinico",
    )
}}

-- This code integrates patient data from VITAI:
-- rj-sms.brutos_prontuario_vitai.paciente (VITAI)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.
-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- Get source data and standardize
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- Patient base table
with
    vitai_tb as (
        select
            {{ remove_accents_upper("cpf") }} as cpf,
            {{ validate_cpf(remove_accents_upper("cpf")) }} as cpf_valido_indicador,
            {{ remove_accents_upper("cns") }} as cns,
            {{ remove_accents_upper("nome") }} as nome,
            {{ remove_accents_upper("telefone") }} as telefone,
            cast("" as string) as email,
            cast(null as string) as cep,
            {{ remove_accents_upper("tipo_logradouro") }} as tipo_logradouro,
            {{ remove_accents_upper("nome_logradouro") }} as logradouro,
            {{ remove_accents_upper("numero") }} as numero,
            {{ remove_accents_upper("complemento") }} as complemento,
            {{ remove_accents_upper("bairro") }} as bairro,
            {{ remove_accents_upper("municipio") }} as cidade,
            {{ remove_accents_upper("uf") }} as estado,
            {{ remove_accents_upper("gid") }} as id_paciente,
            {{ remove_accents_upper("nome_alternativo") }} as nome_social,
            {{ remove_accents_upper("sexo") }} as genero,
            {{ remove_accents_upper("raca_cor") }} as raca,
            {{ remove_accents_upper("nome_mae") }} as mae_nome,
            cast(null as string) as pai_nome,
            date(data_nascimento) as data_nascimento,
            date(data_obito) as obito_data,
            updated_at,
            gid_estabelecimento as id_cnes  -- use gid to get id_cnes from  rj-sms.brutos_prontuario_vitai.estabelecimento
        from {{ ref("raw_prontuario_vitai__paciente") }}  -- `rj-sms-dev`.`brutos_prontuario_vitai`.`paciente`
        where {{ validate_cpf("cpf") }}
    ),

    -- CNS
    vitai_cns_ranked as (
        select
            cpf,
            cns,
            row_number() over (partition by cpf order by updated_at desc) as rank
        from
            (
                select
                    cpf,
                    case when trim(cns) in ('NONE') then null else trim(cns) end as cns,
                    updated_at
                from vitai_tb
            )
        where cns is not null and trim(cns) not in ("")
        group by cpf, cns, updated_at
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
                from (select cpf, cns, rank, 3 as merge_order from vitai_cns_ranked)
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
        order by merge_order asc, rank asc
    ),

    cns_dados as (
        select cpf, array_agg(struct(cns, rank)) as cns from cns_dedup group by cpf
    ),

    -- CONTATO TELEPHONE
    vitai_contato_telefone as (
        select
            cpf,
            tipo,
            case when trim(valor) in ("()", "") then null else valor end as valor,
            rank
        from
            (
                select
                    cpf,
                    'telefone' as tipo,
                    telefone as valor,
                    row_number() over (
                        partition by cpf order by updated_at desc
                    ) as rank
                from vitai_tb
                group by cpf, telefone, updated_at
            )
        where not (trim(valor) in ("()", "") and (rank >= 2))
    ),

    -- CONTATO EMAIL
    vitai_contato_email as (
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
                        partition by cpf order by updated_at desc
                    ) as rank
                from vitai_tb
                group by cpf, email, updated_at
            )
        where not (trim(valor) in ("()", "") and (rank >= 2))
    ),

    telefone_dedup as (
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
                        select cpf, valor, rank, "VITAI" as sistema, 3 as merge_order
                        from vitai_contato_telefone
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
                        select cpf, valor, rank, "VITAI" as sistema, 3 as merge_order
                        from vitai_contato_email
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
                array_agg(struct(t.valor, t.sistema, t.rank)) as telefone,
                array_agg(struct(lower(e.valor), e.sistema, e.rank)) as email
            ) as contato
        from telefone_dedup t
        full outer join email_dedup e on t.cpf = e.cpf
        group by coalesce(t.cpf, e.cpf)
    ),

    -- ENDEREÇO
    vitai_endereco as (
        select
            cpf,
            case when cep in ("NONE") then null else cep end as cep,
            case
                when tipo_logradouro in ("NONE") then null else tipo_logradouro
            end as tipo_logradouro,
            case
                when logradouro in ("NONE") then null else logradouro
            end as logradouro,
            case when numero in ("NONE") then null else numero end as numero,
            case
                when complemento in ("NONE") then null else complemento
            end as complemento,
            case when bairro in ("NONE") then null else bairro end as bairro,
            case when cidade in ("NONE") then null else cidade end as cidade,
            case when estado in ("NONE") then null else estado end as estado,
            cast(updated_at as string) as datahora_ultima_atualizacao,
            row_number() over (partition by cpf order by updated_at desc) as rank
        from vitai_tb
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
                            "VITAI" as sistema,
                            3 as merge_order
                        from vitai_endereco
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
                    lower(tipo_logradouro),
                    {{ proper_br("logradouro") }} as logradouro,
                    numero,
                    lower(complemento) as complemento,
                    {{ proper_br("bairro") }} as bairro,
                    {{ proper_br("cidade") }} as cidade,
                    {{ proper_br("estado") }} as estado,
                    timestamp(
                        datahora_ultima_atualizacao
                    ) as datahora_ultima_atualizacao,
                    sistema,
                    rank
                )
            ) as endereco
        from endereco_dedup
        group by cpf
    ),

    -- PRONTUARIO
    vitai_prontuario as (
        select
            cpf,
            'VITAI' as sistema,
            id_cnes,
            id_paciente,
            row_number() over (partition by cpf order by updated_at desc) as rank
        from
            (
                select pc.updated_at, pc.cpf, pc.id_paciente, es.cnes as id_cnes,
                from vitai_tb pc
                join
                    {{ ref("raw_prontuario_vitai__m_estabelecimento") }} es
                    on pc.id_cnes = es.gid
            )
        group by cpf, id_cnes, id_paciente, updated_at
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
                            "VITAI" as sistema,
                            id_cnes,
                            id_paciente,
                            rank,
                            3 as merge_order
                        from vitai_prontuario vi
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

    -- PACIENTE DADOS
    vitai_paciente as (
        select
            cpf,
            cpf_valido_indicador,
            {{ proper_br("nome") }} as nome,
            {{ proper_br("nome_social") }} as nome_social,
            data_nascimento,
            case
                when genero = "M"
                then initcap("MASCULINO")
                when genero = "F"
                then initcap("FEMININO")
                else null
            end as genero,
            case
                when raca in ("NONE", "NAO INFORMADO", "SEM INFORMACAO")
                then null
                when raca in ("PRETO", "NEGRO")
                then initcap("PRETA")
                else initcap(raca)
            end as raca,
            case when obito_data is not null then true else null end as obito_indicador,
            obito_data,
            case when mae_nome in ("NONE") then null else mae_nome end as mae_nome,
            pai_nome,
            row_number() over (partition by cpf order by updated_at) as rank
        from vitai_tb
        group by
            cpf,
            pai_nome,
            nome,
            nome_social,
            data_nascimento,
            genero,
            obito_data,
            mae_nome,
            updated_at,
            cpf_valido_indicador,
            case when obito_data is not null then true else null end,
            case
                when raca in ("NONE", "NAO INFORMADO", "SEM INFORMACAO")
                then null
                when raca in ("PRETO", "NEGRO")
                then initcap("PRETA")
                else initcap(raca)
            end
    ),

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
                "VITAI" as sistema
            ) as metadados
        from vitai_paciente
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
        from vitai_paciente pc
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
            ct.contato,
            ed.endereco,
            pt.prontuario,
            struct(current_timestamp() as created_at) as metadados
        from paciente_dados pd
        left join cns_dados cns on pd.cpf = cns.cpf
        left join contato_dados ct on pd.cpf = ct.cpf
        left join endereco_dados ed on pd.cpf = ed.cpf
        left join prontuario_dados pt on pd.cpf = pt.cpf
    )

select *
from paciente_integrado
