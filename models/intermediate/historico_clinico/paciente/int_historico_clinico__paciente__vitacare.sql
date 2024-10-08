{{
    config(
        alias="paciente_vitacare",
        materialized="table",
        schema="intermediario_historico_clinico",
    )
}}

-- This code integrates patient data from VITACARE:
-- rj-sms.brutos_prontuario_vitacare.paciente (VITACARE)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.
-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- Get source data and standardize
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- Patient base table
with
    vitacare_tb as (
        select
            {{ remove_accents_upper("cpf") }} as cpf,
            {{ validate_cpf(remove_accents_upper("cpf")) }} as cpf_valido_indicador,
            {{ remove_accents_upper("cns") }} as cns,
            {{ remove_accents_upper("nome") }} as nome,
            {{ remove_accents_upper("cnes_unidade") }} as id_cnes,  -- use cnes_unidade to get name from  rj-sms.saude_dados_mestres.estabelecimento
            {{ remove_accents_upper("codigo_ine_equipe_saude") }} as id_ine,
            {{ remove_accents_upper("telefone") }} as telefone,
            {{ remove_accents_upper("email") }} as email,
            {{ remove_accents_upper("endereco_cep") }} as cep,
            {{ remove_accents_upper("endereco_tipo_logradouro") }} as tipo_logradouro,
            {{
                remove_accents_upper(
                    'REGEXP_EXTRACT(endereco_logradouro, r"^(.*?)(?:\d+.*)?$")'
                )
            }} as logradouro,
            {{
                remove_accents_upper(
                    'REGEXP_EXTRACT(endereco_logradouro, r"\b(\d+)\b")'
                )
            }} as numero,
            {{
                remove_accents_upper(
                    'REGEXP_REPLACE(endereco_logradouro, r"^.*?\d+\s*(.*)$", r"\1")'
                )
            }} as complemento,
            {{ remove_accents_upper("endereco_bairro") }} as bairro,
            {{ remove_accents_upper("endereco_municipio") }} as cidade,
            {{ remove_accents_upper("endereco_estado") }} as estado,
            {{ remove_accents_upper("id") }} as id_paciente,
            {{ remove_accents_upper("nome_social") }} as nome_social,
            {{ remove_accents_upper("sexo") }} as genero,
            {{ remove_accents_upper("raca_cor") }} as raca,
            {{ remove_accents_upper("nome_mae") }} as mae_nome,
            {{ remove_accents_upper("nome_pai") }} as pai_nome,
            date(data_obito) as obito_data,
            date(data_nascimento) as data_nascimento,
            data_atualizacao_vinculo_equipe,  -- Change to data_atualizacao_vinculo_equipe
            updated_at,
            cadastro_permanente
        from {{ ref("raw_prontuario_vitacare__paciente") }}  -- `rj-sms-dev`.`brutos_prontuario_vitacare`.`paciente`
        where
            cpf is not null
            and not regexp_contains({{ remove_accents_upper("cpf") }}, r'[A-Za-z]')
            and trim({{ remove_accents_upper("cpf") }}) != ""
    -- AND tipo = "rotineiro"
    -- AND cpf = cpf_filter
    ),

    -- CNS
    vitacare_cns_ranked as (
        select
            cpf,
            cns,
            row_number() over (
                partition by cpf, cns
                order by
                    data_atualizacao_vinculo_equipe desc,
                    cadastro_permanente desc,
                    updated_at desc
            ) as rank,
        from
            (
                select
                    cpf,
                    case when trim(cns) in ('NONE') then null else trim(cns) end as cns,
                    data_atualizacao_vinculo_equipe,
                    cadastro_permanente,
                    updated_at
                from vitacare_tb
            )
        where cns is not null
        group by
            cpf, cns, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
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
                from (select cpf, cns, rank, 1 as merge_order from vitacare_cns_ranked)
                order by merge_order asc, rank asc
            )
        where dedup_rank = 1
        order by merge_order asc, rank asc
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

    -- EQUIPE SAUDE FAMILIA VITACARE: Extracts and ranks family health teams
    -- clinica da familia
    vitacare_clinica_familia as (
        select
            vc.cpf,
            vc.id_cnes,
            {{ proper_estabelecimento("e.nome_limpo") }} as nome,
            if(array_length(e.telefone) > 0, e.telefone[offset(0)], null) as telefone,
            vc.data_atualizacao_vinculo_equipe,
            row_number() over (
                partition by vc.cpf
                order by
                    vc.data_atualizacao_vinculo_equipe desc,
                    vc.cadastro_permanente desc,
                    vc.updated_at desc
            ) as rank
        from vitacare_tb vc
        join {{ ref("dim_estabelecimento") }} e on vc.id_cnes = e.id_cnes
        where vc.id_cnes is not null
        group by
            vc.cpf,
            vc.id_cnes,
            vc.data_atualizacao_vinculo_equipe,
            vc.cadastro_permanente,
            vc.updated_at,
            e.nome_limpo,
            if(array_length(e.telefone) > 0, e.telefone[offset(0)], null)

    ),

    -- medicos data
    medicos_data as (
        select
            e.id_ine,
            array_agg(
                struct(p.id_profissional_sus, {{ proper_br("p.nome") }} as nome)
            ) as medicos
        from {{ ref("dim_equipe") }} e  -- `rj-sms-dev`.`saude_dados_mestres`.`equipe_profissional_saude` 
        left join unnest(e.medicos) as medico_id
        left join
            {{ ref("dim_profissional_saude") }} p  -- `rj-sms-dev`.`saude_dados_mestres`.`profissional_saude`
            on medico_id = p.id_profissional_sus
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

    vitacare_equipe_saude_familia as (
        select
            vc.cpf,
            vc.id_ine,
            {{ proper_br("e.nome_referencia") }} as nome,
            e.telefone,
            m.medicos,
            en.enfermeiros,
            vc.data_atualizacao_vinculo_equipe as datahora_ultima_atualizacao,
            row_number() over (
                partition by vc.cpf
                order by
                    vc.data_atualizacao_vinculo_equipe desc,
                    vc.cadastro_permanente desc,
                    vc.updated_at desc
            ) as rank
        from vitacare_tb vc
        join
            {{ ref("dim_equipe") }} e  -- `rj-sms-dev`.`saude_dados_mestres`.`equipe_profissional_saude` 
            on vc.id_ine = e.id_ine
        left join medicos_data m on vc.id_ine = m.id_ine
        left join enfermeiros_data en on vc.id_ine = en.id_ine
        where vc.id_ine is not null
        group by
            vc.cpf,
            vc.id_ine,
            e.telefone,
            m.medicos,
            en.enfermeiros,
            vc.data_atualizacao_vinculo_equipe,
            vc.cadastro_permanente,
            vc.updated_at,
            e.nome_referencia
    ),

    equipe_saude_familia_dados as (
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
        from vitacare_equipe_saude_familia ef
        left join vitacare_clinica_familia cf on ef.cpf = cf.cpf
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
                    telefones as valor_original,
                    {{ padronize_telefone("telefones") }} as valor,
                    row_number() over (
                        partition by cpf
                        order by
                            data_atualizacao_vinculo_equipe desc,
                            cadastro_permanente desc,
                            updated_at desc
                    ) as rank
                from vitacare_tb
                group by
                    cpf,
                    telefone,
                    data_atualizacao_vinculo_equipe,
                    cadastro_permanente,
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
                            cadastro_permanente desc,
                            updated_at desc
                    ) as rank
                from vitacare_tb
                group by
                    cpf,
                    email,
                    data_atualizacao_vinculo_equipe,
                    cadastro_permanente,
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
                            "VITACARE" as sistema,
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
                        select cpf, valor, rank, "VITACARE" as sistema, 1 as merge_order
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
                        t.sistema,
                        t.rank
                    )
                ) as telefone,
                array_agg(struct(e.valor, e.sistema, e.rank)) as email
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
                    cadastro_permanente desc,
                    updated_at desc
            ) as rank
        from vitacare_tb
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
            cadastro_permanente,
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
                            "VITACARE" as sistema,
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

    -- PRONTUARIO
    vitacare_prontuario as (
        select
            cpf,
            'VITACARE' as sistema,
            id_cnes,
            id_paciente,
            row_number() over (
                partition by cpf
                order by
                    data_atualizacao_vinculo_equipe desc,
                    cadastro_permanente desc,
                    updated_at desc
            ) as rank
        from vitacare_tb
        group by
            cpf,
            id_cnes,
            id_paciente,
            data_atualizacao_vinculo_equipe,
            cadastro_permanente,
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
                            "VITACARE" as sistema,
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
        select cpf, array_agg(struct(sistema, id_cnes, id_paciente, rank)) as prontuario
        from prontuario_dedup
        group by cpf
    ),

    -- PACIENTE DADOS
    vitacare_paciente as (
        select
            cpf,
            cpf_valido_indicador,
            {{ proper_br("nome") }} as nome,
            case
                when nome_social in ('') then null else {{ proper_br("nome_social") }}
            end as nome_social,
            date(data_nascimento) as data_nascimento,
            case
                when genero in ("M", "MALE")
                then initcap("MASCULINO")
                when genero in ("F", "FEMALE")
                then initcap("FEMININO")
                else null
            end as genero,
            case
                when trim(raca) in ("", "NAO INFORMADO", "SEM INFORMACAO")
                then null
                else initcap(raca)
            end as raca,
            case
                when obito_data is null
                then false
                when obito_data is not null
                then true
                else null
            end as obito_indicador,
            obito_data as obito_data,
            case when mae_nome in ("NONE") then null else mae_nome end as mae_nome,
            pai_nome,
            row_number() over (
                partition by cpf
                order by
                    data_atualizacao_vinculo_equipe desc,
                    cadastro_permanente desc,
                    updated_at desc
            ) as rank
        from vitacare_tb
        group by
            cpf,
            nome,
            nome_social,
            cpf,
            data_nascimento,
            genero,
            raca,
            obito_data,
            mae_nome,
            pai_nome,
            updated_at,
            cadastro_permanente,
            data_atualizacao_vinculo_equipe,
            cpf_valido_indicador
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
                "VITACARE" as sistema
            ) as metadados
        from vitacare_paciente
        group by cpf
    ),

    paciente_dados as (
        select
            pc.cpf,
            array_agg(
                struct(
                    cpf_valido_indicador,
                    nome,
                    nome_social,
                    data_nascimento,
                    genero,
                    raca,
                    obito_indicador,
                    obito_data,
                    mae_nome,
                    pai_nome,
                    rank,
                    pm.metadados
                )
            ) as dados
        from vitacare_paciente pc
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
            struct(current_timestamp() as created_at) as metadados
        from paciente_dados pd
        left join cns_dados cns on pd.cpf = cns.cpf
        left join equipe_saude_familia_dados esf on pd.cpf = esf.cpf
        left join contato_dados ct on pd.cpf = ct.cpf
        left join endereco_dados ed on pd.cpf = ed.cpf
        left join prontuario_dados pt on pd.cpf = pt.cpf
    )

select *
from paciente_integrado
