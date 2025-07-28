{{
    config(
        alias="paciente_smsrio",
        materialized="table",
        schema="intermediario_historico_clinico",
    )
}}

-- This code integrates patient data from smsrio:
-- rj-sms.brutos_plataforma_smsrio.paciente (smsrio)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.
-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";
-- smsrio: Patient base table
with
    smsrio_tb as (
        select
            {{ remove_accents_upper("cpf") }} as cpf,
            {{ validate_cpf(remove_accents_upper("cpf")) }} as cpf_valido_indicador,
            {{ remove_accents_upper("cns_lista") }} as cns_lista,
            {{ remove_accents_upper("nome") }} as nome,
            split(telefone_lista, ',') as telefone_lista,
            {{ remove_accents_upper("email") }} as email,
            {{ padronize_cep(remove_accents_upper("endereco_cep")) }} as cep,
            {{ remove_accents_upper("endereco_tipo_logradouro") }} as tipo_logradouro,
            {{ remove_accents_upper("endereco_logradouro") }} as logradouro,
            {{ remove_accents_upper("endereco_numero") }} as numero,
            {{ remove_accents_upper("endereco_complemento") }} as complemento,
            {{ remove_accents_upper("endereco_bairro") }} as bairro,
            {{ remove_accents_upper("endereco_municipio_codigo") }} as cidade,
            {{ remove_accents_upper("endereco_uf") }} as estado,
            {{ remove_accents_upper("cpf") }} as id_paciente,
            cast(null as string) as nome_social,
            {{ remove_accents_upper("sexo") }} as genero,
            {{ remove_accents_upper("raca_cor") }} as raca,
            {{ remove_accents_upper("nome_mae") }} as mae_nome,
            {{ remove_accents_upper("nome_pai") }} as pai_nome,
            date(data_nascimento) as data_nascimento,
            date(data_obito) as obito_data,
            {{ remove_accents_upper("obito") }} as obito_indicador,
            updated_at,
            cast(null as string) as id_cnes
        from {{ ref("raw_plataforma_smsrio__paciente_cadastro") }}  -- `rj-sms-dev`.`brutos_plataforma_smsrio`.`paciente`
        where {{ validate_cpf("cpf") }}
    ),

    all_cpfs as (select distinct cpf from smsrio_tb),

    -- CNS
    smsrio_cns_ranked as (
        select
            cpf,
            case when trim(cns) in ('NONE') then null else trim(cns) end as cns,
            row_number() over (partition by cpf order by updated_at desc, pos_lista asc) as rank_dupl, -- Ordenação levando em consideração a data de atualização e a posição no array de cns
        from
            (
                select cpf, cns, updated_at, pos_lista
                from
                    smsrio_tb,
                    unnest(
                        split(
                            replace(replace(replace(cns_lista, '[', ''), ']', ''), '"', ''),
                            ','
                        )
                    ) as cns with offset as pos_lista -- Obtendo o elemento e a posição no array
            )
        group by cpf, cns, updated_at, pos_lista

    ),

    -- CNS Dados
    cns_dedup as (
        select
            cpf,
            cns,
            row_number() over (
                partition by cpf order by rank_dupl asc
            ) as rank
        from smsrio_cns_ranked
        qualify row_number() over (
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

    -- CONTATO TB
    smsrio_contato_tb as (
        select
            cpf,
            telefone,
            pos_lista, -- Posição do telefone no array de telefones
            case
                when regexp_contains(telefone, r'@')
                then regexp_replace(trim(lower(telefone)), r'(\.com).*', '.com')
                else email
            end as email,
            updated_at
        from
            smsrio_tb,
            unnest(telefone_lista) as telefone with offset as pos_lista -- Obtendo o elemento e a posição no array
    ),

    -- CONTATO TELEPHONE
    smsrio_contato_telefone as (
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
            rank_dupl,
            pos_lista
        from
            (
                select
                    cpf,
                    'telefone' as tipo,
                    telefone as valor_original,
                    {{ padronize_telefone("telefone") }} as valor,
                    row_number() over (
                        partition by cpf order by pos_lista asc
                    ) as rank_dupl,
                    pos_lista
                from smsrio_contato_tb
                group by cpf, telefone, updated_at, pos_lista
            )
        where not (trim(valor) in ("NONE", "NULL", "") and (rank_dupl >= 2))
    ),

    -- CONTATO smsrio: Extracts and ranks email
    smsrio_contato_email as (
        select
            cpf,
            tipo,
            case
                when trim(valor) in ("NONE", "NULL", "") then null else valor
            end as valor,
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
                from smsrio_contato_tb
                group by cpf, email, updated_at
            )
        where not (trim(valor) in ("NONE", "NULL", "") and (rank_dupl >= 2))
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
            "smsrio" as sistema
        from smsrio_contato_telefone
        qualify row_number() over (
                        partition by cpf, valor order by rank_dupl asc, pos_lista asc
                    )  = 1
        order by rank_dupl asc
    ),

    email_dedup as (
        select
            cpf,
            valor,
            row_number() over (
                partition by cpf order by rank_dupl asc
            ) as rank,
            "smsrio" as sistema
        from smsrio_contato_email
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
    smsrio_endereco as (
        select
            sms.cpf,
            sms.cep,
            case
                when sms.tipo_logradouro in ("NONE", "")
                then null
                else sms.tipo_logradouro
            end as tipo_logradouro,
            sms.logradouro,
            sms.numero,
            sms.complemento,
            sms.bairro,
            case
                when sms.cidade in ("NONE", "")
                then null
                when regexp_contains(sms.cidade, r'^\d+$')
                then {{ remove_accents_upper("bd.nome") }}
                else sms.cidade
            end as cidade,
            sms.estado,
            cast(sms.updated_at as string) as datahora_ultima_atualizacao,
            row_number() over (partition by cpf order by updated_at desc) as rank_dupl
        from smsrio_tb sms
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` bd
            on sms.cidade = bd.id_municipio_6
        group by
            sms.cpf,
            sms.cep,
            sms.tipo_logradouro,
            sms.logradouro,
            sms.numero,
            sms.complemento,
            sms.bairro,
            sms.cidade,
            bd.nome,
            sms.estado,
            sms.updated_at
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
            "smsrio" as sistema
        from smsrio_endereco
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
    smsrio_prontuario as (
        select
            cpf,
            'smsrio' as sistema,
            id_cnes,
            id_paciente,
            row_number() over (partition by cpf order by updated_at desc) as rank_dupl
        from smsrio_tb
        group by cpf, id_cnes, id_paciente, updated_at
    ),

    prontuario_dedup as (
        select
            cpf,
            sistema,
            id_cnes,
            id_paciente,
            row_number() over (
                partition by cpf order by rank_dupl asc
            ) as rank
        from smsrio_prontuario vi
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
    smsrio_paciente as (
        select
            cpf,
            cpf_valido_indicador,
            {{ proper_br("nome") }} as nome,
            {{ proper_br("nome_social") }} as nome_social,
            data_nascimento,
            case
                when genero = "1"
                then initcap("MASCULINO")
                when genero = "2"
                then initcap("FEMININO")
                else null
            end as genero,
            case
                when raca in ("NONE", "None", "NAO INFORMADO", "SEM INFORMACAO")
                then null
                else initcap(raca)
            end as raca,
            case
                when obito_indicador = "0"
                then false
                when obito_indicador = "1"
                then true
                else false
            end as obito_indicador,
            obito_data,
            case when mae_nome in ("NONE") then null else mae_nome end as mae_nome,
            pai_nome,
            row_number() over (partition by cpf order by updated_at) as rank
        from smsrio_tb
        group by
            cpf,
            nome,
            nome_social,
            cpf,
            data_nascimento,
            genero,
            obito_indicador,
            obito_data,
            mae_nome,
            pai_nome,
            updated_at,
            cpf_valido_indicador,
            case
                when raca in ("NONE", "None", "NAO INFORMADO", "SEM INFORMACAO")
                then null
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
                "smsrio" as sistema
            ) as metadados
        from smsrio_paciente
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
        from smsrio_paciente pc
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
