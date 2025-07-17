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
            * except (id, source_updated_at),
            id as id_paciente,
            source_updated_at as updated_at,
            row_number() over (
                partition by cpf
                order by cadastro_permanente_indicador desc, updated_at_rank desc
            ) as rank --rankeria registro mais novo e confiavel do cpf
        from {{ ref("raw_prontuario_vitacare__paciente") }}
        where {{ validate_cpf("cpf") }}
        qualify row_number() over (
                partition by cpf order by cadastro_permanente_indicador desc, updated_at_rank desc
            ) = 1 -- deduplica cpf, mantendo o mais novo

    ),
    paciente_com_cadastro_permanente as (
        select * from paciente where cadastro_permanente_indicador = true
    ),

    paciente_com_equipe as (
        select *
        from paciente_com_cadastro_permanente
        where equipe_familia_indicador = true
    ),

    all_cpfs as (select distinct cpf from paciente),

    -- CNS
    cns_ranked as (
        select
            cpf,
            cns,
            row_number() over ( -- ranking para deduplicacao de cns iguais
                partition by cpf, cns
                order by
                    data_atualizacao_vinculo_equipe desc,
                    cadastro_permanente_indicador desc,
                    updated_at desc
            ) as deduped_rank,
            row_number() over ( -- rankeia dentre os mesmos cpf os registros mais novos de cns
                partition by cpf
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
    cns_validated as (
        select cns, {{ validate_cns("cns") }} as cns_valido_indicador,
        from (select distinct cns from cns_ranked where deduped_rank=1)
    ),

    cns_dados as (
        select cpf, array_agg(struct(cd.cns, cv.cns_valido_indicador, cd.rank)) as cns
        from ( select * from cns_ranked where deduped_rank=1) cd
        join cns_validated cv on cd.cns = cv.cns
        group by cpf
    ),

    -- EQUIPE SAUDE FAMILIA vitacare: Extracts and ranks family health teams
    -- clinica da familia
    source_clinica_familia as (
        -- funciona como deduplicação
        -- cria tabela de cnes onde o paciente ja foi atendido
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
            -- enriquece e adiciona ranking de clinica baseado em data de cadastro
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
        -- ultima equipe de cada cnes que o paciente ja foi atendido
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
        -- enriquece e adiciona ranking de clinica baseado em data de cadastro
        select 
            f.cpf,
            f.id_ine,
            f.id_cnes,
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
        left join {{ ref("dim_equipe") }} e on f.id_ine = e.id_ine
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
                order by ef.rank asc
            ) as equipe_saude_familia
        from equipe_saude_familia_enriquecida ef
        left join dim_clinica_familia cf on ef.cpf = cf.cpf and ef.id_cnes = cf.id_cnes
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
                then substr(valor, 1, 2)  -- Deixa apenas os primeiros 2 digitos (DD)
                else null
            end as ddd,
            case
                when length(valor) in (8, 9)
                then valor  -- Numeros com 8 ou 9 digitos, permanece o valor original
                when length(valor) = 10
                then substr(valor, 3, 8)  -- Deixa apenas os 8 ultimos digitos (descarta os dois primeiros)
                when length(valor) = 11
                then substr(valor, 3, 9)  -- Deixa apenas os 9 ultimos digitos (descarta os dois primeiros)
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
                        partition by cpf
                        order by
                            data_atualizacao_vinculo_equipe desc,
                            cadastro_permanente_indicador desc,
                            updated_at desc
                    ) as rank_dupl
                from paciente
                group by
                    cpf,
                    telefone,
                    data_atualizacao_vinculo_equipe,
                    cadastro_permanente_indicador,
                    updated_at
            )
        where not (trim(valor) in ("()", "") and (rank_dupl >= 2))
    ),

    -- CONTATO EMAIL
    vitacare_contato_email as (
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
                        partition by cpf
                        order by
                            data_atualizacao_vinculo_equipe desc,
                            cadastro_permanente_indicador desc,
                            updated_at desc
                    ) as rank_dupl
                from paciente
                group by
                    cpf,
                    email,
                    data_atualizacao_vinculo_equipe,
                    cadastro_permanente_indicador,
                    updated_at
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
            "vitacare" as sistema,
            row_number() over (
                partition by cpf order by rank_dupl asc
            ) as rank,
        from vitacare_contato_telefone
        qualify  row_number() over (
                partition by cpf, valor order by rank_dupl asc
            ) =1 
        order by rank asc
    ),

    email_dedup as (
        select
            cpf,
            valor,
            row_number() over (
                partition by cpf order by rank_dupl asc
            ) as rank,
            "vitacare" as sistema
        from vitacare_contato_email
        qualify row_number() over (
                partition by cpf, valor order by rank_dupl asc
            ) = 1
        order by rank asc
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
    vitacare_endereco as (
        select
            cpf,
            endereco_cep as cep,
            endereco_tipo_logradouro as tipo_logradouro,
            endereco_logradouro as logradouro,
            endereco_numero as numero,
            endereco_complemento as complemento,
            endereco_bairro as bairro,
            case
                when regexp_contains(endereco_municipio, r'\d')
                then trim(regexp_replace(endereco_municipio, r'\[.*', ''))
                else endereco_municipio
            end as cidade,
            endereco_estado as estado,
            cast(
                data_atualizacao_vinculo_equipe as string
            ) as datahora_ultima_atualizacao,
            row_number() over (
                partition by cpf
                order by
                    data_atualizacao_vinculo_equipe desc,
                    cadastro_permanente_indicador desc,
                    updated_at desc
            ) as rank_dupl
        from paciente
        where endereco_logradouro is not null
        group by
            cpf,
            endereco_cep,
            endereco_tipo_logradouro,
            endereco_logradouro,
            endereco_numero,
            endereco_complemento,
            endereco_bairro,
            endereco_municipio,
            endereco_estado,
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
                partition by cpf order by rank_dupl asc
            ) as rank,
            "vitacare" as sistema
        from vitacare_endereco
        qualify row_number() over (
                        partition by cpf, datahora_ultima_atualizacao
                        order by rank_dupl asc
                    )= 1
        order by rank asc
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
            id as id_paciente,
            row_number() over (
                partition by cpf
                order by source_updated_at
            ) as rank
        from {{ ref("raw_prontuario_vitacare__paciente") }}
    ),

    prontuario_dados as (
        select
            cpf,
            array_agg(
                struct(lower(sistema) as sistema, id_cnes, id_paciente, rank)
            ) as prontuario
        from vitacare_prontuario
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
                count(distinct sexo) as qtd_sexos,
                count(distinct raca) as qtd_racas,
                count(distinct obito_indicador) as qtd_obitos_indicadores,
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
                    CASE 
                        WHEN lower(sexo) = 'female' THEN 'feminino'
                        WHEN lower(sexo) = 'male' THEN 'masculino'
                        ELSE null
                    END  as genero,
                    lower(raca) as raca,
                    obito_indicador,
                    safe_cast(null as date) as obito_data,
                    {{ proper_br("mae_nome") }} as mae_nome,
                    {{ proper_br("pai_nome") }} as pai_nome,
                    rank,
                    pm.metadados
                )
            ) as dados
        from paciente pc
        join paciente_metadados as pm on pc.cpf = pm.cpf
        group by pc.cpf
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
            struct(datetime(current_timestamp(),'America/Sao_Paulo') as created_at) as metadados,
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
