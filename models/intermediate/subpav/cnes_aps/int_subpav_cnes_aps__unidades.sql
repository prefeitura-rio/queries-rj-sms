{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__unidades',
        materialized="table",
        tags=["subpav", "cnes_aps"]
    )
}}

with fonte as (
    select
        *
    from {{ref("raw_gdb_cnes__lfces004")}}
),

tipo_unidade as (
    select
        data_particao,
        nullif(TP_UNID_ID, '') as tipo_unidade_id,
        nullif(DESCRICAO, '') as tipo_unidade_descricao
    from {{ ref("raw_gdb_cnes__nfces010") }}
    qualify row_number() over (
        partition by data_particao, nullif(TP_UNID_ID, '')
        order by _loaded_at desc
    ) = 1
),

motivo_desabilitacao as (
    select
        data_particao,
        nullif(CD_MOTIVO_DESAB, '') as motivo_desativacao_unidade_id,
        nullif(DS_MOTIVO_DESAB, '') as motivo_desativacao_unidade
    from {{ ref("raw_gdb_cnes__nfces049") }}
    qualify row_number() over (
        partition by data_particao, nullif(CD_MOTIVO_DESAB, '')
        order by _loaded_at desc
    ) = 1
),

natureza_juridica as (
    select
        data_particao,
        nullif(CO_NATUREZA_JUR, '') as natureza_juridica_id,
        nullif(DS_NATUREZA_JUR, '') as natureza_juridica
    from {{ ref("raw_gdb_cnes__nfces085") }}
    qualify row_number() over (
        partition by data_particao, nullif(CO_NATUREZA_JUR, '')
        order by _loaded_at desc
    ) = 1
),

cnes_validos as (
    select
        data_particao,
        lpad(nullif(regexp_replace(CNES, r'[^0-9]', ''), ''), 7, '0') as cnes,
        nullif(COD_MUN, '') as cod_municipio_cnes,
        nullif(COMPETENCIA, '') as competencia_cnes,
        nullif(STATUS_ESTAB, '') as status_estabelecimento_cnes,
        nullif(CD_MOTIVO_DESAB, '') as motivo_desativacao_cnes_id,
        nullif(TP_GESTAO, '') as tipo_gestao_cnes,
        nullif(ST_ESTRUTURA, '') as estrutura_estabelecimento
    from {{ ref("raw_gdb_cnes__lfces057") }}
    qualify row_number() over (
        partition by
            data_particao,
            lpad(nullif(regexp_replace(CNES, r'[^0-9]', ''), ''), 7, '0')
        order by _loaded_at desc
    ) = 1
),

extraido as (
    select
        -- metadados da carga
        data_particao,
        ano_particao,
        mes_particao,
        format_date('%Y-%m', data_particao) as competencia_mes,
        _loaded_at,
        _source_file,

        -- identificação
        lpad(nullif(regexp_replace(CNES, r'[^0-9]', ''), ''), 7, '0') as cnes,
        nullif(UNIDADE_ID, '') as unidade_id_original,
        nullif(DIST_SANIT, '') as ap_original,
        nullif(NOME_FANTA, '') as nome_fanta,
        nullif(R_SOCIAL, '') as razao_social,

        -- localização/contato
        nullif(LOGRADOURO, '') as logradouro,
        nullif(NUMERO, '') as numero,
        nullif(COMPLEMENT, '') as complemento,
        nullif(BAIRRO, '') as bairro,
        nullif(COD_CEP, '') as cep,
        nullif(TELEFONE, '') as telefone,
        nullif(FAX, '') as fax,
        nullif(E_MAIL, '') as email,
        nullif(NO_URL, '') as url,

        safe_cast(nullif(NU_LATITUDE, '') as float64) as latitude,
        safe_cast(nullif(NU_LONGITUDE, '') as float64) as longitude,
        safe_cast(nullif(DT_ATU_GEO, '') as date) as dt_atualiza_geo,
        nullif(NO_USUARIO_GEO, '') as usuario_geo,

        -- classificação CNES
        nullif(TP_UNID_ID, '') as tipo_unidade_id,
        nullif(COD_TURNAT, '') as turno_atendimento_id,
        nullif(CD_MOTIVO_DESAB, '') as motivo_desativacao_unidade_id,
        nullif(CO_NATUREZA_JUR, '') as natureza_juridica_id,
        nullif(CO_TIPO_ESTABELECIMENTO, '') as tipo_estabelecimento_id,
        nullif(CO_ATIVIDADE_PRINCIPAL, '') as atividade_principal_id,
        nullif(CO_TIPO_UNIDADE, '') as tipo_unidade_federal_id,
        nullif(CO_TIPO_ABRANGENCIA, '') as tipo_abrangencia_id,

        -- gestão/status
        nullif(CODMUNGEST, '') as cod_municipio_gestor,
        nullif(SIGESTGEST, '') as sigla_gestao,
        nullif(TP_GESTAO, '') as tipo_gestao_id,
        nullif(STATUS, '') as status,
        nullif(STATUSMOV, '') as status_movimento,
        nullif(PFPJ_IND, '') as pessoa_fisica_juridica_id,
        nullif(NIVEL_DEP, '') as nivel_dependencia_id,

        -- flags/documentos
        nullif(TP_ESTAB_SEMPRE_ABERTO, '') as tipo_estab_sempre_aberto,
        nullif(ST_CONEXAOINTERNET, '') as possui_conexao_internet_original,
        nullif(ST_CONTRATO_FORMALIZADO, '') as contrato_formalizado_original,
        nullif(ST_COWORKING, '') as coworking_original,

        -- datas
        safe_cast(nullif(DATA_ATU, '') as date) as dt_atualiza,
        safe_cast(nullif(DATA_EXPED, '') as date) as dt_expedicao,
        safe_cast(nullif(DT_VAL_LIC_SANI, '') as date) as dt_validade_licenca_sanitaria,
        safe_cast(nullif(DT_VALIDACAO, '') as datetime) as dt_validacao,
        safe_cast(nullif(DT_ATUALIZACAO_ORIGEM, '') as date) as dt_atualizacao_origem,
        safe_cast(nullif(DT_CMTP_INICIO, '') as date) as dt_cmtp_inicio,
        safe_cast(nullif(DT_CMTP_FIM, '') as date) as dt_cmtp_fim,

        -- diretor / documentos
        lpad(nullif(regexp_replace(CPFDIRETORCLINICO, r'[^0-9]', ''), ''), 11, '0') as cpf_diretor_clinico,
        nullif(REGDIRETORCLINICO, '') as registro_diretor_clinico,
        lpad(nullif(regexp_replace(CNPJ, r'[^0-9]', ''), ''), 14, '0') as cnpj,
        lpad(nullif(regexp_replace(CNPJ_MANT, r'[^0-9]', ''), ''), 14, '0') as cnpj_mantenedora,

        -- controle
        nullif(USUARIO, '') as usuario_atualizacao,
        nullif(NMUSUARIOEMUSO, '') as usuario_em_uso,
        nullif(CHKSUM, '') as checksum

    from fonte
),

tratado as (
    select
        e.*,

        upper(coalesce(e.nome_fanta, '')) as nome_fanta_upper,

        case
            when regexp_replace(coalesce(ap_original, ''), r'[^0-9]', '') in ('10', '010', '0010') then 10
            when regexp_replace(coalesce(ap_original, ''), r'[^0-9]', '') in ('21', '021', '0021') then 21
            when regexp_replace(coalesce(ap_original, ''), r'[^0-9]', '') in ('22', '022', '0022') then 22
            when regexp_replace(coalesce(ap_original, ''), r'[^0-9]', '') in ('31', '031', '0031') then 31
            when regexp_replace(coalesce(ap_original, ''), r'[^0-9]', '') in ('32', '032', '0032') then 32
            when regexp_replace(coalesce(ap_original, ''), r'[^0-9]', '') in ('33', '033', '0033') then 33
            when regexp_replace(coalesce(ap_original, ''), r'[^0-9]', '') in ('40', '040', '0040') then 40
            when regexp_replace(coalesce(ap_original, ''), r'[^0-9]', '') in ('51', '051', '0051') then 51
            when regexp_replace(coalesce(ap_original, ''), r'[^0-9]', '') in ('52', '052', '0052') then 52
            when regexp_replace(coalesce(ap_original, ''), r'[^0-9]', '') in ('53', '053', '0053') then 53
            else null
        end as ap,

        case
            when tipo_estab_sempre_aberto = 'N' then 0
            when tipo_estab_sempre_aberto is null then null
            else 1
        end as estab_sempre_aberto,

        case
            when possui_conexao_internet_original = 'S' then 1
            when possui_conexao_internet_original = 'N' then 0
            else null
        end as possui_conexao_internet,

        case
            when contrato_formalizado_original = 'S' then 1
            when contrato_formalizado_original = 'N' then 0
            else null
        end as contrato_formalizado,

        case
            when coworking_original = 'S' then 1
            when coworking_original = 'N' then 0
            else null
        end as coworking,

        case
            when motivo_desativacao_unidade_id is null then 1
            else 0
        end as unidade_ativa

    from extraido e
),

com_esfera as (
    select
        t.*,

        case
            when natureza_juridica_id = '1031' then 3 -- Municipal
            when natureza_juridica_id in ('1015', '1074', '1104', '1139') then 1 -- Federal
            when natureza_juridica_id in ('1023', '1147', '1260') then 2 -- Estadual
            when natureza_juridica_id is not null then 4 -- Privada / demais
            else null
        end as esfera_administrativa_id

    from tratado t
),

classificado as (
    select
        t.*,

        case
            when ap = 10 then '1.0'
            when ap = 21 then '2.1'
            when ap = 22 then '2.2'
            when ap = 31 then '3.1'
            when ap = 32 then '3.2'
            when ap = 33 then '3.3'
            when ap = 40 then '4.0'
            when ap = 51 then '5.1'
            when ap = 52 then '5.2'
            when ap = 53 then '5.3'
            else null
        end as ap_formatada,

        case
            when cnes = '5462886'
                and esfera_administrativa_id = 3
                then 'SMS'

            when safe_cast(tipo_unidade_id as int64) = 68
                and ap = 0
                and esfera_administrativa_id = 3
                then 'CAP'

            -- CF: precisa bloquear privadas que começam com CF
            when esfera_administrativa_id = 3
                and safe_cast(tipo_unidade_id as int64) in (1, 2)
                and (
                    regexp_contains(nome_fanta_upper, r'(^|[^A-Z0-9])CF([^A-Z0-9]|$)')
                    or regexp_contains(nome_fanta_upper, r'CL[IÍ]NICA DA FAM[IÍ]LIA')
                )
                then 'CF'

            -- CMS: municipal + nome, mas sem exigir tipo_unidade_id 1/2
            when esfera_administrativa_id = 3
                and (
                    regexp_contains(nome_fanta_upper, r'(^|[^A-Z0-9])CMS([^A-Z0-9]|$)')
                    or regexp_contains(nome_fanta_upper, r'CENTRO MUNICIPAL DE SA[ÚU]DE')
                )
                then 'CMS'

            -- CSE: pode ser federal, mas precisa ser tipo básico
            when safe_cast(tipo_unidade_id as int64) in (1, 2)
                and (
                    regexp_contains(nome_fanta_upper, r'(^|[^A-Z0-9])CSE([^A-Z0-9]|$)')
                    or regexp_contains(nome_fanta_upper, r'CENTRO SA[ÚU]DE ESCOLA')
                )
                then 'CSE'

            when esfera_administrativa_id = 3
                and regexp_contains(nome_fanta_upper, r'CAPS ?III')
                then 'CAPSIII'

            when esfera_administrativa_id = 3
                and regexp_contains(nome_fanta_upper, r'CAPSI')
                then 'CAPSI'

            when esfera_administrativa_id = 3
                and regexp_contains(nome_fanta_upper, r'(^|[^A-Z0-9])CAPS([^A-Z0-9]|$)')
                then 'CAPS'

            when esfera_administrativa_id = 3
                and regexp_contains(nome_fanta_upper, r'POLICL')
                then 'POLICLINICA'

            when esfera_administrativa_id = 3
                and regexp_contains(nome_fanta_upper, r'(^|[^A-Z0-9])UPA([^A-Z0-9]|$)')
                then 'UPA'

            when esfera_administrativa_id = 3
                and regexp_contains(nome_fanta_upper, r'(^|[^A-Z0-9])CER([^A-Z0-9]|$)')
                then 'CER'

            when esfera_administrativa_id = 3
                and regexp_contains(nome_fanta_upper, r'HOSP')
                then 'HOSPITAL'

            else 'OUTROS'
        end as tipo_unidade_sms,

        case
            when cnes = '5462886'
                or (safe_cast(tipo_unidade_id as int64) = 68 and ap = 0)
                or regexp_contains(nome_fanta_upper, r'SECRETARIA')
                or regexp_contains(nome_fanta_upper, r'COORDENADORIA')
                then 4
            when safe_cast(tipo_unidade_id as int64) in (1, 2) then 1
            when safe_cast(tipo_unidade_id as int64) in (4, 20, 21, 36, 61, 70, 71, 73) then 2
            when safe_cast(tipo_unidade_id as int64) in (5, 7) then 3
            else null
        end as nivel_atencao_id,

        case
            when cnes = '5462886'
                or (safe_cast(tipo_unidade_id as int64) = 68 and ap = 0)
                or regexp_contains(nome_fanta_upper, r'SECRETARIA')
                or regexp_contains(nome_fanta_upper, r'COORDENADORIA')
                then 'GESTAO'
            when safe_cast(tipo_unidade_id as int64) in (1, 2) then 'ATENCAO_PRIMARIA'
            when safe_cast(tipo_unidade_id as int64) in (4, 20, 21, 36, 61, 70, 71, 73) then 'ATENCAO_SECUNDARIA'
            when safe_cast(tipo_unidade_id as int64) in (5, 7) then 'ATENCAO_TERCIARIA'
            else 'NAO_CLASSIFICADO'
        end as nivel_atencao

    from com_esfera t
),

com_dimensoes as (
    select
        c.*,

        tu.tipo_unidade_descricao,
        md.motivo_desativacao_unidade,
        nj.natureza_juridica,

        case
            when c.tipo_unidade_sms in ('CF', 'CMS', 'CSE') then 1
            else 0
        end as is_unidade_aps,

        case
            when c.tipo_unidade_sms = 'CF' then 1 else 0
        end as is_clinica_familia,

        case
            when c.tipo_unidade_sms = 'CMS' then 1 else 0
        end as is_centro_municipal_saude,

        case
            when c.tipo_unidade_sms = 'CSE' then 1 else 0
        end as is_centro_saude_escola,
        
        cv.cod_municipio_cnes,
        cv.competencia_cnes,
        cv.status_estabelecimento_cnes,
        cv.motivo_desativacao_cnes_id,
        cv.tipo_gestao_cnes,
        cv.estrutura_estabelecimento,

        case
            when coalesce(cv.cod_municipio_cnes, c.cod_municipio_gestor) = '330455' then 1
            else 0
        end as is_municipio_rio

    from classificado c
    left join tipo_unidade tu
        on c.data_particao = tu.data_particao
        and c.tipo_unidade_id = tu.tipo_unidade_id
    left join motivo_desabilitacao md
        on c.data_particao = md.data_particao
        and c.motivo_desativacao_unidade_id = md.motivo_desativacao_unidade_id
    left join natureza_juridica nj
        on c.data_particao = nj.data_particao
        and c.natureza_juridica_id = nj.natureza_juridica_id
    left join cnes_validos cv
        on c.data_particao = cv.data_particao
        and c.cnes = cv.cnes
),

finalizado as (
    select
        *,

        case
            when unidade_ativa = 1
                and is_municipio_rio = 1
                and tipo_unidade_sms in ('CF', 'CMS', 'CSE')
                then 1
            else 0
        end as is_unidade_aps_panorama

    from com_dimensoes
),

deduplicado as (
    select *
    from finalizado
    qualify row_number() over (
        partition by data_particao, coalesce(cnes, unidade_id_original)
        order by _loaded_at desc, dt_atualiza desc
    ) = 1
)

select
    -- identificação principal
    cnes,
    unidade_id_original,
    nome_fanta,
    nome_fanta_upper,
    razao_social,

    -- território
    ap,
    ap_formatada,
    ap_original,
    cod_municipio_cnes,
    cod_municipio_gestor,
    is_municipio_rio,

    -- classificação SMS / Panorama
    tipo_unidade_sms,
    is_unidade_aps_panorama,
    is_unidade_aps,
    is_clinica_familia,
    is_centro_municipal_saude,
    is_centro_saude_escola,

    -- tipo / nível / esfera
    tipo_unidade_id,
    tipo_unidade_descricao,
    tipo_unidade_federal_id,
    tipo_estabelecimento_id,
    atividade_principal_id,
    tipo_abrangencia_id,
    nivel_atencao_id,
    nivel_atencao,
    natureza_juridica_id,
    natureza_juridica,
    esfera_administrativa_id,

    -- status / gestão / situação
    unidade_ativa,
    motivo_desativacao_unidade_id,
    motivo_desativacao_unidade,
    motivo_desativacao_cnes_id,
    status,
    status_movimento,
    status_estabelecimento_cnes,
    tipo_gestao_id,
    tipo_gestao_cnes,
    sigla_gestao,
    competencia_cnes,
    estrutura_estabelecimento,

    -- localização/endereço
    logradouro,
    numero,
    complemento,
    bairro,
    cep,
    latitude,
    longitude,

    -- contato
    telefone,
    fax,
    email,
    url,

    -- funcionamento / flags operacionais
    turno_atendimento_id,
    tipo_estab_sempre_aberto,
    estab_sempre_aberto,
    possui_conexao_internet_original,
    possui_conexao_internet,
    contrato_formalizado_original,
    contrato_formalizado,
    coworking_original,
    coworking,

    -- documentos / responsáveis
    pessoa_fisica_juridica_id,
    nivel_dependencia_id,
    cnpj,
    cnpj_mantenedora,
    cpf_diretor_clinico,
    registro_diretor_clinico,

    -- datas de negócio/CNES
    dt_atualiza,
    dt_expedicao,
    dt_validade_licenca_sanitaria,
    dt_validacao,
    dt_atualiza_geo,
    dt_atualizacao_origem,
    dt_cmtp_inicio,
    dt_cmtp_fim,

    -- controle CNES
    usuario_atualizacao,
    usuario_em_uso,
    usuario_geo,
    checksum,

    -- metadados da carga
    competencia_mes,
    data_particao,
    ano_particao,
    mes_particao,
    _loaded_at,
    _source_file

from deduplicado
