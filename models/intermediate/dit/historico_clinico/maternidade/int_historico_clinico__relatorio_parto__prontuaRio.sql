{{
    config(
        schema="intermediario_historico_clinico",
        alias="relatorio_parto_prontuaRio",
        materialized="table",
        meta={"owner": "herian"}
    )
}}


with 
relatorio_parto as (
    select 
        cnes,
        gid_registro,
        evolucao_data,
        upper(descricao) as descricao,
        loaded_at
    from {{ ref('raw_prontuario_prontuaRio__evolucao') }}
    where regexp_contains(upper(descricao), r'(?i)relat[oó]rio\s+de\s+parto')
),

    
blocos_extraidos as (
  select 
    gid_registro,
    evolucao_data,
    descricao,
    regexp_extract(descricao, r'(?i)(data\s+do\s+parto\s*:?\s*[\s\S]+?dura[cç][aã]o)') as trecho_data_parto,
    regexp_extract(descricao, r'(?is)((?:iii\s*[-–]\s*)?procedimentos[\s\S]*?)bole[dt][ei]?[mn]?\s+operat[oó]rio') as bloco_recem_nascido,
    regexp_extract(descricao, r'(?is)bole[dt][ei]?[mn]?\s+operat[oó]rio\s*([\s\S]*)') as bloco_boletim,
    loaded_at
  from relatorio_parto
  qualify row_number() over(partition by gid_registro order by evolucao_data desc) = 1
),


trecho_padronizado as (
  select
    gid_registro,
    descricao,
    evolucao_data,
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(
                          regexp_replace(
                            upper(trecho_data_parto),
                          r'\bJANEIRO\b|\bJANERO\b|\bJAN\.?\b', '01'),
                        r'\bFEVEREIRO\b|\bFEVEREI?RO\b|\bFEV\.?\b', '02'),
                      r'\bMAR[CÇ]O\b|\bMARSO\b|\bMAR\.?\b', '03'),
                    r'\bABRIL\b|\bABRI\.?\b', '04'),
                  r'\bMAIO\b|\bMAI\.?\b', '05'),
                r'\bJUNHO\b|\bJUN\.?\b', '06'),
              r'\bJULHO\b|\bJUL\.?\b', '07'),
            r'\bAGOSTO\b|\bAGO\.?\b', '08'),
          r'\bSE[TP][ET]EMBRO\b|\bSETEM\.?\b|\bSETEMBRO\.?\b', '09'),
        r'\bOUTUBRO\b|\bOUT\.?\b', '10'),
      r'\bNOVEMBRO\b|\bNOVEMBR\.?\b|\bNOV\.?\b', '11'),
    r'\bDEZEMBRO\b|\bDEZ\.?\b', '12') as trecho
  from blocos_extraidos
),


-- Caso 1: "Dia 12 Mês 4 Ano 2024"
caso_1 as (
  select
    gid_registro,
    regexp_extract(trecho, r'(?i)data\s+do\s+parto\s*:?\s*dia?\.?\s+(\d{1,2})') as dia,
    regexp_extract(trecho, r'(?i)data\s+do\s+parto\s*:?\s*dia?\.?\s+\d{1,2}\s+m[eê]s\.?\s+(\d{1,2})') as mes,
    regexp_extract(trecho, r'(?i)data\s+do\s+parto\s*:?\s*dia?\.?\s+\d{1,2}\s+m[eê]s\.?\s+\d{1,2}\s+ano\.?\s+(\d{2,4})') as ano,
    regexp_extract(trecho, r'\d{2}:\d{2}(?::\d{2})?') as horario
  from trecho_padronizado
),

caso_1_formatado as (
  select 
    gid_registro,  
    horario,
    concat(lpad(dia,2,'0'), '/', lpad(mes,2,'0'), '/', ano) as data_extraida
  from caso_1
  where dia is not null
),


-- Caso 2: DD/MM/AA, DD/MM/AAAA, DD-MM-AAAA, DD.MM.AAAA
caso_2_formatado as (
  select
    gid_registro,
    regexp_extract(trecho, r'(\d{1,2}[./-]\d{1,2}[./-](?:\d{4}|\d{2}))') as data_extraida,
    regexp_extract(trecho, r'\d{2}:\d{2}(?::\d{2})?') as horario
  from trecho_padronizado
  where regexp_contains(trecho, r'\d{1,2}[./-]\d{1,2}[./-](?:\d{4}|\d{2})')
),

data_parto_extraida as (
  select gid_registro, data_extraida, horario from caso_2_formatado
  union all
  select gid_registro, data_extraida, horario from caso_1_formatado
),


recem_nascido as (
  select
    gid_registro,
        case
      when regexp_extract(descricao, r'(?i)CARDIOTOCOGRAFIA\s+INTRAPARTO\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)CARDIOTOCOGRAFIA\s+INTRAPARTO\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as cardiotocografia_intraparto,

    case
      when regexp_extract(descricao, r'(?i)ANTIBI[ÓO]TICOTERAPIA\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)ANTIBI[ÓO]TICOTERAPIA\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as antibioticoterapia,

    case
      when regexp_extract(descricao, r'(?i)ANTI[\s\-]+HIPERTENSIVO\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)ANTI[\s\-]+HIPERTENSIVO\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as anti_hipertensivo,

    case
      when regexp_extract(descricao, r'(?i)OCITOCINA\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)OCITOCINA\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as ocitocina,

    case
      when regexp_extract(descricao, r'(?i)MEP[AE]R\S+\s+OU\s+BENZODIAZEP\S+\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)MEP[AE]R\S+\s+OU\s+BENZODIAZEP\S+\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as meperidina_benzodiazepinico,

    case
      when regexp_extract(descricao, r'(?i)AMNI?O\S+\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)AMNI?O\S+\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as amniotomia,

    case
      when regexp_extract(descricao, r'(?i)ANESTESIA\s+LOCO[\s\-]*REGIONAL\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)ANESTESIA\s+LOCO[\s\-]*REGIONAL\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as anestesia_loco_regional,

    case
      when regexp_extract(descricao, r'(?i)ANESTESIA\s+PERIDURAL\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)ANESTESIA\s+PERIDURAL\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as anestesia_peridural,

    case
      when regexp_extract(descricao, r'(?i)ANESTESIA\s+RAQUI\S*\s*\(\s*X\s*\)\s*S') != '' then 'SIM' 
      when regexp_extract(descricao, r'(?i)ANESTESIA\s+RAQUI\S*\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as anestesia_raquidiana,

    case
      when regexp_extract(descricao, r'(?i)ANESTESIA\s+GERAL\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)ANESTESIA\s+GERAL\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO' 
    end as anestesia_geral,

    case
      when regexp_extract(descricao, r'(?i)TRANSFUS\S+\s+SAN[GQ]U\S+\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)TRANSFUS\S+\s+SAN[GQ]U\S+\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as transfusao_sanguinea,

    case
      when regexp_extract(descricao, r'(?i)ULTRASSONOGRAFIA\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)ULTRASSONOGRAFIA\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO' 
    end as ultrassonografia,

    case
      when regexp_extract(descricao, r'(?i)OUTROS\s*\(\s*X\s*\)\s*S') != '' then 'SIM' 
      when regexp_extract(descricao, r'(?i)OUTROS\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as outros_procedimentos,

    -- RECÉM-NASCIDO 
    regexp_extract(descricao, r'(?i)SEXO\s+(MASCULINO|FEMININO)') as sexo_rn,

    regexp_extract(descricao, r'(?i)PESO\s+AO\s+NASCER\s*(\d+)?') as peso_rn,

    coalesce(
        regexp_extract(descricao, r'(?i)1\S*\s+MINUTO\s*(\d+)'),
        regexp_extract(descricao, r'(?i)APGAR.*?1\S*\s*MIN\S*\s*(\d+)')
    ) as apgar_1min,

    coalesce(
        regexp_extract(descricao, r'(?i)[25]\S*\s+MINUTO\s*(\d+)'),
        regexp_extract(descricao, r'(?i)APGAR.*?5\S*\s*MIN\S*\s*(\d+)')
    ) as apgar_5min, 

    case
      when regexp_extract(descricao, r'(?i)ANOMALIA\s+CONG\S+\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)ANOMALIA\s+CONG\S+\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as anomalia_congenita,

    case
      when regexp_extract(descricao, r'(?i)TOCOTRAUMATISMO\s*\(\s*X\s*\)\s*S') != '' then 'SIM'
      when regexp_extract(descricao, r'(?i)TOCOTRAUMATISMO\s*\(.*?\)\s*S\S*\s*\(\s*X\s*\)\s*N') != '' then 'NAO'
    end as tocotraumatismo
  from blocos_extraidos where bloco_recem_nascido is not null
),


boletim_operatorio as (
  select 
    gid_registro,
    regexp_extract(bloco_boletim, r'(?i)diagn[oó]stico\s+pr[eé]-?\s*operat[oó]rio\s*:?\s*([^\n\r]+)') as diagnostico_pre_operatorio,
    regexp_extract(bloco_boletim, r'(?i)diagn[oó]stico\s+cir[uú]rgico\s*:?\s*([^\n\r]+)') as diagnostico_cirurgico,
    regexp_extract(bloco_boletim, r'(?i)(?:cirurgia|procedimento)\s+realizado[a]?\s*:?\s*([^\n\r]+)') as cirurgia_realizada,
    regexp_extract(bloco_boletim, r'(?i)dura[cç][aã]o\s*:?\s*([^\n\r]+)') as duracao,
    regexp_extract(bloco_boletim, r'(?is)descri[cç][aã]o\s+da\s+cirurgia\s*:?\s*([\s\S]*)') as descricao_cirurgia
  from blocos_extraidos
),


paciente as (
  select 
    gid_registro,
    paciente_cpf,
    cns,
    paciente_nome,
    paciente_telefone
  from {{ ref('raw_prontuario_prontuaRio__internacao_cadastro') }}
),


extracao_final as (
  select 
    be.gid_registro,
    evolucao_data,
    
    -- Dados da paciente
    paciente_cpf,
    cns,
    paciente_telefone, --TODO: Limpar telefones (talvez na camada raw)
    paciente_nome as nome_cadastro,

    -- Evolução
    data_extraida,
    descricao,
    case 
          when length(data_extraida) = 10 then safe.parse_date('%d/%m/%Y', data_extraida)
          when length(data_extraida) = 8 then safe.parse_date('%d/%m/%y', data_extraida)
      else null
    end as data_parto,
    horario,

    -- Campos de Procedimentos
    cardiotocografia_intraparto,
    antibioticoterapia,
    anti_hipertensivo,
    ocitocina,
    meperidina_benzodiazepinico,
    amniotomia,
    anestesia_loco_regional,
    anestesia_peridural,
    anestesia_raquidiana,
    anestesia_geral,
    transfusao_sanguinea,
    ultrassonografia,
    outros_procedimentos,

    -- Campos de Recém-Nascido
    sexo_rn,
    peso_rn,
    apgar_1min,
    apgar_5min,
    anomalia_congenita,
    tocotraumatismo,
    
    -- Campos de Boletim Operatório
    diagnostico_pre_operatorio,
    diagnostico_cirurgico,
    cirurgia_realizada,
    duracao,
    descricao_cirurgia,
    
    -- Metadados
    current_date('America/Sao_Paulo') as processed_at,
    loaded_at,
    cast(evolucao_data as date) as data_particao

  from blocos_extraidos be
  left join data_parto_extraida using(gid_registro)
  left join recem_nascido using(gid_registro)
  left join boletim_operatorio using(gid_registro)
  left join paciente using(gid_registro)
)


select *
from extracao_final
where paciente_cpf is not null
