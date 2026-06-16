{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__profissionais',
        materialized = "table",
        partition_by = {
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by = ["cpf", "cns"],
        tags = ["subpav", "cnes_aps"]
    )
}}

with fonte as (
    select
        _source_file,
        safe_cast(_loaded_at as timestamp) as loaded_at,
        safe_cast(data_particao as date) as data_particao,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,

        PROF_ID,
        CPF_PROF,
        PISPASEP,
        NOME_PROF,
        NO_SOCIAL,
        NOME_MAE,
        NOME_PAI,
        DATA_NASC,
        COD_MUN,
        SEXO,
        CD_RACA,
        CODESCOLAR,
        IND_NACIO,
        CO_NACIONALIDADE,
        NOME_PAIS,
        CD_PAIS,
        CD_TP_LOGR,
        LOGRADOURO,
        NUMERO,
        COMPLEMENT,
        BAIRRODIST,
        COD_CEP,
        COD_MUN_RES,
        UF_RES,
        CO_PAIS_RESID,
        SIGLA_UF,
        TELEFONE,
        NO_EMAIL,
        COD_CNS,
        STATUS,
        STATUSMOV,
        DATA_ATU,
        USUARIO,
        NMUSUARIOEMUSO,
        CHKSUM,
        CO_ETNIA,
        ST_NMPROF_CADSUS,
        NUM_IDENT,
        CODORGEMIS,
        SIGLA_EST,
        DTEMIIDENT,
        NUM_LIVRO,
        NUM_FOLHA,
        NUM_TERMO,
        COD_CERTID,
        DATA_EMISS,
        NOME_CARTO,
        CTPS_NUMER,
        SERIE,
        SIGESTCTPS,
        DTEMISCTPS,
        DATA_ENTRA,
        DT_NATUR,
        NU_CARTEIRA_HAB,
        DT_EMIS_CARTEIRA_HAB,
        UF_CARTEIRA_HAB,
        CO_SEQ_INCLUSAO

    from {{ ref("raw_gdb_cnes__lfces018") }}
),

extraido as (
    select
        
        nullif(cast(PROF_ID as string), '') as profissional_id_original,
        lpad(
            nullif(regexp_replace(cast(CPF_PROF as string), r'[^0-9]', ''), ''),
            11,
            '0'
        ) as cpf,
        lpad(
            nullif(regexp_replace(cast(COD_CNS as string), r'[^0-9]', ''), ''),
            15,
            '0'
        ) as cns,
        nullif(cast(PISPASEP as string), '') as pis_pasep,

        
        nullif(cast(NOME_PROF as string), '') as nome_profissional,
        nullif(cast(NO_SOCIAL as string), '') as nome_social,
        nullif(cast(NOME_MAE as string), '') as nome_mae,
        nullif(cast(NOME_PAI as string), '') as nome_pai,

        
        safe_cast(nullif(cast(DATA_NASC as string), '') as date) as dt_nascimento,
        nullif(cast(SEXO as string), '') as sexo_id_original,
        nullif(cast(CD_RACA as string), '') as raca_cor_id_original,
        nullif(cast(CODESCOLAR as string), '') as nivel_escolaridade_id_original,
        nullif(cast(IND_NACIO as string), '') as nacionalidade_indicador_original,
        nullif(cast(CO_NACIONALIDADE as string), '') as nacionalidade_id_original,
        nullif(cast(NOME_PAIS as string), '') as nome_pais_origem,
        nullif(cast(CD_PAIS as string), '') as pais_origem_id,
        nullif(cast(CO_ETNIA as string), '') as etnia_id,

        
        nullif(cast(COD_MUN as string), '') as municipio_nascimento_id,
        nullif(cast(COD_MUN_RES as string), '') as municipio_residencia_id,
        nullif(cast(UF_RES as string), '') as uf_residencia,
        nullif(cast(CO_PAIS_RESID as string), '') as pais_residencia_id,
        nullif(cast(SIGLA_UF as string), '') as uf_identidade,
        nullif(cast(CD_TP_LOGR as string), '') as tipo_logradouro_id,
        nullif(cast(LOGRADOURO as string), '') as logradouro,
        nullif(cast(NUMERO as string), '') as numero,
        nullif(cast(COMPLEMENT as string), '') as complemento,
        nullif(cast(BAIRRODIST as string), '') as bairro,
        nullif(cast(COD_CEP as string), '') as cep,

        
        nullif(cast(TELEFONE as string), '') as telefone,
        nullif(cast(NO_EMAIL as string), '') as email,

        
        nullif(cast(NUM_IDENT as string), '') as numero_identidade,
        nullif(cast(CODORGEMIS as string), '') as orgao_emissor_identidade,
        nullif(cast(SIGLA_EST as string), '') as uf_orgao_emissor_identidade,
        safe_cast(nullif(cast(DTEMIIDENT as string), '') as date) as dt_emissao_identidade,
        nullif(cast(NUM_LIVRO as string), '') as numero_livro_certidao,
        nullif(cast(NUM_FOLHA as string), '') as numero_folha_certidao,
        nullif(cast(NUM_TERMO as string), '') as numero_termo_certidao,
        nullif(cast(COD_CERTID as string), '') as certidao_id,
        safe_cast(nullif(cast(DATA_EMISS as string), '') as date) as dt_emissao_certidao,
        nullif(cast(NOME_CARTO as string), '') as nome_cartorio,
        nullif(cast(CTPS_NUMER as string), '') as ctps_numero,
        nullif(cast(SERIE as string), '') as ctps_serie,
        nullif(cast(SIGESTCTPS as string), '') as ctps_uf,
        safe_cast(nullif(cast(DTEMISCTPS as string), '') as date) as dt_emissao_ctps,
        nullif(cast(NU_CARTEIRA_HAB as string), '') as carteira_habilitacao_numero,
        safe_cast(nullif(cast(DT_EMIS_CARTEIRA_HAB as string), '') as date) as dt_emissao_carteira_habilitacao,
        nullif(cast(UF_CARTEIRA_HAB as string), '') as uf_carteira_habilitacao,

        
        safe_cast(nullif(cast(DATA_ENTRA as string), '') as date) as dt_entrada_brasil,
        safe_cast(nullif(cast(DT_NATUR as string), '') as date) as dt_naturalizacao,

        
        nullif(cast(STATUS as string), '') as status,
        nullif(cast(STATUSMOV as string), '') as status_movimento,
        safe_cast(nullif(cast(DATA_ATU as string), '') as date) as dt_atualiza,
        nullif(cast(USUARIO as string), '') as usuario_atualizacao,
        nullif(cast(NMUSUARIOEMUSO as string), '') as usuario_em_uso,
        nullif(cast(CHKSUM as string), '') as checksum,
        nullif(cast(ST_NMPROF_CADSUS as string), '') as nome_profissional_cadsus_original,
        nullif(cast(CO_SEQ_INCLUSAO as string), '') as sequencial_inclusao,

        
        format_date('%Y-%m', data_particao) as competencia_mes,
        data_particao,
        ano_particao,
        mes_particao,
        loaded_at,
        _source_file

    from fonte
),

tratado as (
    select
        *,

        case
            when upper(sexo_id_original) in ('M', '1') then 1
            when upper(sexo_id_original) in ('F', '2') then 2
            else 0
        end as sexo_id,

        case
            when upper(sexo_id_original) in ('M', '1') then 'MASCULINO'
            when upper(sexo_id_original) in ('F', '2') then 'FEMININO'
            else 'NAO_INFORMADO'
        end as sexo,

        safe_cast(nullif(raca_cor_id_original, '') as int64) as raca_cor_id,
        safe_cast(nullif(nivel_escolaridade_id_original, '') as int64) as nivel_escolaridade_id,

        case
            when nacionalidade_indicador_original = '1' then 'BRASILEIRA'
            when nacionalidade_indicador_original = '2' then 'ESTRANGEIRA'
            when nacionalidade_indicador_original = '3' then 'NATURALIZADA'
            else null
        end as nacionalidade,

        case
            when nome_profissional_cadsus_original = 'S' then 1
            when nome_profissional_cadsus_original = 'N' then 0
            else null
        end as nome_profissional_cadsus,

        case
            when regexp_contains(cpf, r'^[0-9]{11}$')
                and cpf != '00000000000'
                then 1
            else 0
        end as cpf_valido,

        case
            when regexp_contains(cns, r'^[0-9]{15}$')
                and cns != '000000000000000'
                then 1
            else 0
        end as cns_preenchido

    from extraido
    where cpf is not null
),

deduplicado as (
    select *
    from tratado
    qualify row_number() over (
        partition by data_particao, cpf
        order by
            loaded_at desc,
            dt_atualiza desc,
            profissional_id_original desc
    ) = 1
)

select
    cpf,
    cns,
    pis_pasep,
    profissional_id_original,

    nome_profissional,
    nome_social,
    nome_mae,
    nome_pai,

    dt_nascimento,
    sexo_id,
    sexo,
    sexo_id_original,
    raca_cor_id,
    raca_cor_id_original,
    nivel_escolaridade_id,
    nivel_escolaridade_id_original,
    nacionalidade,
    nacionalidade_indicador_original,
    nacionalidade_id_original,
    nome_pais_origem,
    pais_origem_id,
    etnia_id,

    municipio_nascimento_id,
    municipio_residencia_id,
    uf_residencia,
    pais_residencia_id,
    uf_identidade,
    tipo_logradouro_id,
    logradouro,
    numero,
    complemento,
    bairro,
    cep,

    telefone,
    email,

    numero_identidade,
    orgao_emissor_identidade,
    uf_orgao_emissor_identidade,
    dt_emissao_identidade,
    numero_livro_certidao,
    numero_folha_certidao,
    numero_termo_certidao,
    certidao_id,
    dt_emissao_certidao,
    nome_cartorio,
    ctps_numero,
    ctps_serie,
    ctps_uf,
    dt_emissao_ctps,
    carteira_habilitacao_numero,
    dt_emissao_carteira_habilitacao,
    uf_carteira_habilitacao,

    dt_entrada_brasil,
    dt_naturalizacao,

    cpf_valido,
    cns_preenchido,
    nome_profissional_cadsus,
    nome_profissional_cadsus_original,
    
    status,
    status_movimento,
    dt_atualiza,
    usuario_atualizacao,
    usuario_em_uso,
    checksum,
    sequencial_inclusao,

    competencia_mes,
    data_particao,
    ano_particao,
    mes_particao,
    loaded_at,
    _source_file

from deduplicado
