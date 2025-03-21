{{
    config(
        alias="paciente_vitai",
        materialized="table",
        schema="intermediario_historico_clinico",
    )
}}


-- This code integrates patient data from vitai:
-- rj-sms.brutos_prontuario_vitai.paciente (vitai)
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

    all_cpfs as (select distinct cpf from vitai_tb),

    -- CNS
    vitai_cns_ranked as (
        select
            cpf,
            cns,
            row_number() over (partition by cpf order by updated_at desc) as rank_dupl
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
                partition by cpf order by rank_dupl asc
            ) as rank
        from vitai_cns_ranked
        qualify  row_number() over (
                        partition by cpf, cns order by rank_dupl asc
                    ) = 1
        order by rank_dupl asc
    ),
    cns_validated as (
        select cns, {{ validate_cns("cns") }} as cns_valido_indicador,
        from (select distinct cns from cns_dedup)
    ),
    cns_dados as (
        select cpf, array_agg(struct(cd.cns, cv.cns_valido_indicador, cd.rank)) as cns
        from cns_dedup cd
        join cns_validated cv on cd.cns = cv.cns
        group by cpf
    ),
    -- CONTATO TELEPHONE
    vitai_contato_telefone as (
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
            rank_dupl
        from
            (
                select
                    cpf,
                    'telefone' as tipo,
                    telefone as valor_original,
                    {{ padronize_telefone("telefone") }} as valor,
                    row_number() over (
                        partition by cpf order by updated_at desc
                    ) as rank_dupl
                from vitai_tb
                group by cpf, telefone, updated_at
            )
        where not (trim(valor) in ("()", "") and (rank_dupl >= 2))
    ),

    -- CONTATO EMAIL
    vitai_contato_email as (
        select
            cpf,
            tipo,
            case when trim(valor) in ("()", "") then null else valor end as valor,
            rank_dupl
        from
            (
                select
                    cpf,
                    'email' as tipo,
                    email as valor,
                    row_number() over (
                        partition by cpf order by updated_at desc
                    ) as rank_dupl
                from vitai_tb
                group by cpf, email, updated_at
            )
        where not (trim(valor) in ("()", "") and (rank_dupl >= 2))
    ),

    telefone_dedup as (
        select
            cpf,
            valor_original,
            ddd,
            valor,
            valor_tipo,
            row_number() over (
                partition by cpf order by rank_dupl asc
            ) as rank,
            "vitai" as sistema
        from vitai_contato_telefone
        qualify row_number() over (
            partition by cpf, valor order by rank_dupl asc
        ) = 1
        order by rank_dupl asc
    ),

    email_dedup as (
        select
            cpf,
            valor,
            row_number() over (
                partition by cpf order by  rank_dupl asc
            ) as rank,
            "vitai" as sistema
        from vitai_contato_email
        qualify row_number() over (
                        partition by cpf, valor order by rank_dupl asc
        ) = 1
        order by rank_dupl asc
    ),

    contato_telefone_dados as (
        select
            t.cpf,
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
            row_number() over (partition by cpf order by updated_at desc) as rank_dupl
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
                partition by cpf order by rank_dupl asc
            ) as rank,
            "vitai" as sistema
        from vitai_endereco
        qualify row_number() over (
            partition by cpf, datahora_ultima_atualizacao
            order by rank_dupl asc
        ) = 1
        order by rank_dupl asc
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
    vitai_prontuario as (
        select
            cpf,
            'vitai' as sistema,
            id_cnes,
            id_paciente,
            row_number() over (partition by cpf order by updated_at desc) as rank_dupl
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
            "vitai" as sistema,
            id_cnes,
            id_paciente,
            row_number() over (
                partition by cpf order by rank_dupl asc
            ) as rank
        from vitai_prontuario vi
        qualify row_number() over (
            partition by cpf, id_cnes, id_paciente
            order by rank_dupl asc
        ) = 1
        order by rank_dupl asc
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
            case when obito_data is not null then true else false end as obito_indicador,
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
                "vitai" as sistema
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
            struct(datetime(current_timestamp(),'America/Sao_Paulo') as created_at) as metadados
        from paciente_dados pd
        left join cns_dados cns on pd.cpf = cns.cpf
        left join contato_dados ct on pd.cpf = ct.cpf
        left join endereco_dados ed on pd.cpf = ed.cpf
        left join prontuario_dados pt on pd.cpf = pt.cpf
    )

select *
from paciente_integrado
