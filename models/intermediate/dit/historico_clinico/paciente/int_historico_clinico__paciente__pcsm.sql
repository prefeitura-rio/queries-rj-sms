{{
    config(
        alias="paciente_pcsm",
        materialized="table",
        schema="intermediario_historico_clinico",
    )
}}

-- Este código integra os dados de pacientes da fonte pcsm:
-- Substitua 'nome_da_tabela_raw_pcsm' pela referência correta da sua tabela bruta.
-- O objetivo é consolidar informações de cadastro, contato, endereço e
-- prontuário em uma única visão, seguindo o padrão do modelo vitai.

-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- Obtém os dados da fonte e padroniza
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- Tabela base de pacientes da fonte PCSM
with
    pcsm_tb as (
        select
            {{ remove_accents_upper("numero_cpf_paciente") }} as cpf,
            {{ validate_cpf(remove_accents_upper("numero_cpf_paciente")) }} as cpf_valido_indicador,
            {{ remove_accents_upper("numero_cartao_saude") }} as cns,
            {{ remove_accents_upper("nome_paciente") }} as nome,
            {{ remove_accents_upper("telefone1_paciente") }} as telefone1,
            {{ remove_accents_upper("telefone2_paciente") }} as telefone2,
            {{ remove_accents_upper("email_paciente") }} as email,
            {{ remove_accents_upper("numero_cep_paciente") }} as cep,
            cast(null as string) as tipo_logradouro,  -- Adicionar se existir na fonte
            {{ remove_accents_upper("logradouro_paciente") }} as logradouro,
            {{ remove_accents_upper("numero_endereco") }} as numero,
            {{ remove_accents_upper("complemento_endereco") }} as complemento,
            {{ remove_accents_upper("bairro_paciente") }} as bairro,
            {{ remove_accents_upper("municipio_endereco") }} as cidade,
            {{ remove_accents_upper("sigla_uf_endereco") }} as estado,
            cast(id_paciente as string) as id_paciente,
            {{ remove_accents_upper("nome_social_paciente") }} as nome_social,
            {{ remove_accents_upper("sexo_paciente") }} as genero,
            {{ remove_accents_upper("descricao_raca_cor_paciente") }} as raca,
            {{ remove_accents_upper("nome_mae_paciente") }} as mae_nome,
            {{ remove_accents_upper("nome_pai_paciente") }} as pai_nome,
            date(data_nascimento_paciente) as data_nascimento,
            date(data_obito_paciente) as obito_data,
            loaded_at, -- Usando loaded_at como referência de atualização
            coalesce(
                id_unidade_caps_referencia,
                id_unidade_atencao_primaria_referencia,
                id_unidade_ambulatorial_referencia
            ) as id_unidade  -- Coalesce para encontrar um id de unidade de referência
        from {{ ref("raw_pcsm_pacientes") }}
        where {{ validate_cpf("numero_cpf_paciente") }}
    ),

    all_cpfs as (select distinct cpf from pcsm_tb where cpf is not null),

    -- CNS
    pcsm_cns_ranked as (
        select
            cpf,
            cns,
            row_number() over (partition by cpf order by loaded_at desc) as rank_dupl
        from
            (
                select
                    cpf,
                    case when trim(cns) in ('NONE', '') then null else trim(cns) end as cns,
                    loaded_at
                from pcsm_tb
            )
        where cns is not null
        group by cpf, cns, loaded_at
    ),

    cns_dedup as (
        select
            cpf,
            cns,
            row_number() over (partition by cpf order by rank_dupl asc) as rank
        from pcsm_cns_ranked
        qualify row_number() over (partition by cpf, cns order by rank_dupl asc) = 1
        order by rank_dupl asc
    ),

    cns_validated as (
        select cns, {{ validate_cns("cns") }} as cns_valido_indicador
        from (select distinct cns from cns_dedup)
    ),

    cns_dados as (
        select cpf, array_agg(struct(cd.cns, cv.cns_valido_indicador, cd.rank)) as cns
        from cns_dedup as cd
        join cns_validated as cv on cd.cns = cv.cns
        group by cpf
    ),

    -- CONTATO (agrega telefone1 e telefone2)
    pcsm_telefones_raw as (
        select cpf, telefone1 as telefone, loaded_at from pcsm_tb where telefone1 is not null
        union all
        select cpf, telefone2 as telefone, loaded_at from pcsm_tb where telefone2 is not null
    ),

    pcsm_contato_telefone as (
        select
            cpf,
            'telefone' as tipo,
            telefone as valor_original,
            {{ padronize_telefone("telefone") }} as valor,
            row_number() over (partition by cpf, telefone order by loaded_at desc) as rank_dupl
        from pcsm_telefones_raw
        group by cpf, telefone, loaded_at
    ),

    telefone_parsed as (
         select
            cpf,
            valor_original,
            case
                when length(valor) in (10, 11) then substr(valor, 1, 2)
                else null
            end as ddd,
            case
                when length(valor) in (8, 9) then valor
                when length(valor) = 10 then substr(valor, 3, 8)
                when length(valor) = 11 then substr(valor, 3, 9)
                else null
            end as valor,
            case
                when length(valor) = 8 then 'fixo'
                when length(valor) = 9 then 'celular'
                when length(valor) = 10 then 'ddd_fixo'
                when length(valor) = 11 then 'ddd_celular'
                else null
            end as valor_tipo,
            rank_dupl
        from pcsm_contato_telefone
    ),

    telefone_dedup as (
        select
            cpf,
            valor_original,
            ddd,
            valor,
            valor_tipo,
            row_number() over (partition by cpf order by rank_dupl asc) as rank,
            "pcsm" as sistema
        from telefone_parsed
        where valor is not null
        qualify row_number() over (partition by cpf, valor order by rank_dupl asc) = 1
        order by rank_dupl asc
    ),

    -- CONTATO EMAIL
    pcsm_contato_email as (
        select
            cpf,
            'email' as tipo,
            case when trim(email) in ("()", "") then null else email end as valor,
            row_number() over (partition by cpf order by loaded_at desc) as rank_dupl
        from pcsm_tb
        where email is not null
        group by cpf, email, loaded_at
    ),

    email_dedup as (
        select
            cpf,
            valor,
            row_number() over (partition by cpf order by rank_dupl asc) as rank,
            "pcsm" as sistema
        from pcsm_contato_email
        qualify row_number() over (partition by cpf, valor order by rank_dupl asc) = 1
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
            ) as telefone
        from telefone_dedup as t
        group by t.cpf
    ),

    contato_email_dados as (
        select
            e.cpf,
            array_agg(
                struct(lower(e.valor) as valor, lower(e.sistema) as sistema, e.rank)
            ) as email
        from email_dedup as e
        group by e.cpf
    ),

    contato_dados as (
        select
            a.cpf,
            struct(
                ctd.telefone, ced.email
            ) as contato
        from all_cpfs as a
        left join contato_email_dados as ced using (cpf)
        left join contato_telefone_dados as ctd using (cpf)
    ),

    -- ENDEREÇO
    pcsm_endereco as (
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
            cast(loaded_at as string) as datahora_ultima_atualizacao,
            row_number() over (
                partition by cpf
                order by loaded_at desc
            ) as rank_dupl
        from pcsm_tb
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
            row_number() over (partition by cpf order by rank_dupl asc) as rank,
            "pcsm" as sistema
        from pcsm_endereco
        qualify row_number() over (partition by cpf, datahora_ultima_atualizacao order by rank_dupl asc) = 1
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
                    timestamp(datahora_ultima_atualizacao) as datahora_ultima_atualizacao,
                    lower(sistema) as sistema,
                    rank
                )
            ) as endereco
        from endereco_dedup
        group by cpf
    ),

    -- PRONTUARIO
    pcsm_prontuario as (
        select
            cpf,
            'pcsm' as sistema,
            id_cnes,
            id_paciente,
            row_number() over (partition by cpf order by loaded_at desc) as rank_dupl
        from (
            select cpf, pcsm.loaded_at, id_paciente, codigo_nacional_estabelecimento_saude as id_cnes
                from pcsm_tb as pcsm
                join {{ ref("raw_pcsm_unidades_saude") }} as u 
                    on pcsm.id_unidade = u.id_unidade_saude
        )
        group by cpf, id_cnes, id_paciente, loaded_at
    ),

    prontuario_dedup as (
        select
            cpf,
            "pcsm" as sistema,
            id_cnes,
            id_paciente,
            row_number() over (partition by cpf order by rank_dupl asc) as rank
        from pcsm_prontuario
        qualify row_number() over (partition by cpf, id_cnes, id_paciente order by rank_dupl asc) = 1
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

    -- DADOS DO PACIENTE
    pcsm_paciente as (
        select
            cpf,
            cpf_valido_indicador,
            {{ proper_br(remove_invalid_names("nome")) }} as nome,
            {{ proper_br(remove_invalid_names(eliminate_babies("nome_social"))) }} as nome_social,
            data_nascimento,
            case
                when upper(genero) = "M" then initcap("MASCULINO")
                when upper(genero) = "F" then initcap("FEMININO")
                else null
            end as genero,
            initcap(raca) as raca,
            case when obito_data is not null then true else false end as obito_indicador,
            obito_data,
            {{ proper_br(remove_invalid_names("mae_nome")) }} as mae_nome,
            {{ proper_br(remove_invalid_names("pai_nome")) }} as pai_nome,
            row_number() over (partition by cpf order by loaded_at) as rank
        from pcsm_tb
    ),
    
    pcsm_paciente_nomes_unicos as (
        select
            * except (nome_social),
            case
                when {{ is_same_name("nome", "nome_social") }} then null
                when {{ is_same_name("mae_nome", "nome_social" )}} then null
                else nome_social
            end as nome_social
        from pcsm_paciente
    ),

    paciente_metadados as (
        select
            cpf,
            struct(
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
                "pcsm" as sistema
            ) as metadados
        from pcsm_paciente_nomes_unicos
        group by cpf
    ),

    paciente_dados as (
        select
            pc.cpf,
            array_agg(
                struct(
                    pc.cpf_valido_indicador,
                    pc.nome,
                    pc.nome_social,
                    pc.data_nascimento,
                    lower(pc.genero) as genero,
                    lower(pc.raca) as raca,
                    pc.obito_indicador,
                    pc.obito_data,
                    pc.mae_nome,
                    pc.pai_nome,
                    pc.rank,
                    pm.metadados
                )
            ) as dados
        from pcsm_paciente_nomes_unicos as pc
        join paciente_metadados as pm on pc.cpf = pm.cpf
        group by pc.cpf
    ),

    -- -- JOIN FINAL: Une todos os dados processados para criar a tabela integrada de pacientes.
    paciente_integrado as (
        select
            pd.cpf,
            cns.cns,
            pd.dados,
            ct.contato,
            ed.endereco,
            pt.prontuario,
            struct(
                datetime(current_timestamp(), 'America/Sao_Paulo') as created_at
            ) as metadados
        from paciente_dados as pd
        left join cns_dados as cns on pd.cpf = cns.cpf
        left join contato_dados as ct on pd.cpf = ct.cpf
        left join endereco_dados as ed on pd.cpf = ed.cpf
        left join prontuario_dados as pt on pd.cpf = pt.cpf
    )

select *
from paciente_integrado