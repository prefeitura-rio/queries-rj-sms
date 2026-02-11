{{ 
    config(
        materialized = 'table',
        alias        = "resultado_exame",
        tags         = ["subpav", "sinanrio", "gal"],
        partition_by = {
            "field": "dt_resultado",
            "data_type": "date"
        },
        cluster_by   = ["paciente_cns", "id_tipo_exame", "dt_resultado"]
    ) 
}}

with base as (
    select
        nullif(
            regexp_replace(
                case
                    when regexp_contains(upper(tipo_doc_paciente_1), r'CPF') then documento_paciente_1
                    when regexp_contains(upper(tipo_doc_paciente_2), r'CPF') then documento_paciente_2
                    else null
                end,
                r'\D',
                ''
            ),
            ''
        ) as paciente_cpf,

        regexp_replace(cns_paciente, r'\D', '') as paciente_cns,
        upper(paciente)                         as nome,
        nullif(trim(codigo_amostra), '')        as codigo_amostra,

        coalesce(
            nullif(trim(cnes_unidade_solicitante), ''),
            nullif(trim(cnes_unidade_notificacao_sinan), ''),
            nullif(trim(cnes_laboratorio_execucao), ''),
            nullif(trim(cnes_laboratorio_responsavel), ''),
            nullif(trim(cnes_laboratorio_cadastro), '')
        ) as cnes,

        case
            when tipo_exame = 'tugexp-pcrtr' then 1
            when tipo_exame = 'tubb-colzn'  then 2
        end as id_tipo_exame,

        coalesce(
            coalesce(safe.parse_date('%d/%m/%Y', data_liberacao),         safe.parse_date('%Y-%m-%d', data_liberacao),         safe.parse_date('%d-%m-%Y', data_liberacao)),
            coalesce(safe.parse_date('%d/%m/%Y', data_processamento),     safe.parse_date('%Y-%m-%d', data_processamento),     safe.parse_date('%d-%m-%Y', data_processamento)),
            coalesce(safe.parse_date('%d/%m/%Y', data_inicio_processamento), safe.parse_date('%Y-%m-%d', data_inicio_processamento), safe.parse_date('%d-%m-%Y', data_inicio_processamento)),
            coalesce(safe.parse_date('%d/%m/%Y', data_recebimento),       safe.parse_date('%Y-%m-%d', data_recebimento),       safe.parse_date('%d-%m-%Y', data_recebimento)),
            coalesce(safe.parse_date('%d/%m/%Y', data_solicitacao),       safe.parse_date('%Y-%m-%d', data_solicitacao),       safe.parse_date('%d-%m-%Y', data_solicitacao))
        ) as dt_resultado,

        upper(tugexp_pcrtr.dna_para_complexo_mycobacterium_tuberculosis) as pcr_dna,
        upper(tugexp_pcrtr.rifampicina)                                  as pcr_rif,
        upper(tubb_colzn.resultado)                                      as ziehl_res,

        loaded_at
    from {{ ref('raw_gal__exames_laboratoriais') }}
    where tipo_exame in ('tugexp-pcrtr','tubb-colzn')
),

classificado as (
    select
        codigo_amostra,
        paciente_cpf,
        paciente_cns,
        nome,
        cnes,
        id_tipo_exame,
        dt_resultado,

        case
            when id_tipo_exame = 1 then
                case
                    when regexp_contains(pcr_dna, r'N[ÃA]O\s*TESTA') then 5
                    when regexp_contains(pcr_dna, r'N[ÃA]O\s*DETEC') then 3
                    when regexp_contains(pcr_dna, r'INCONCLUS|INDETERMIN') then 4
                    when regexp_contains(pcr_dna, r'DETEC') and regexp_contains(pcr_dna, r'TRA[ÇC]') then 4
                    when regexp_contains(pcr_dna, r'DETEC') then
                        case
                            when regexp_contains(pcr_rif, r'RESIST') then 2
                            when pcr_rif is null
                                or trim(pcr_rif) = ''
                                or regexp_contains(pcr_rif, r'SENSI|SUSCET|INDETERMIN') then 1
                            else 1
                        end
                    else null
                end

            when id_tipo_exame = 2 then
                case
                    when regexp_contains(ziehl_res, r'\+{3}') then 1
                    when regexp_contains(ziehl_res, r'\+{2}') then 5
                    when regexp_contains(ziehl_res, r'\+') and not regexp_contains(ziehl_res, r'\+{2,}') then 4

                    when regexp_contains(ziehl_res, r'ENCONTRAD[OA]S?\s+\b[1-9][0-9]*\b\s+B\.A\.A\.R') then 4
                    when regexp_contains(ziehl_res, r'\bPOSITIV') and not regexp_contains(ziehl_res, r'\+') then 4

                    when regexp_contains(ziehl_res, r'AUS[ÊE]NCIA\s+DE\s+B\.A\.A\.R')
                        or regexp_contains(ziehl_res, r'ENCONTRAD[OA]S?\s+\b0\b\s+B\.A\.A\.R')
                        or regexp_contains(ziehl_res, r'\bNEGATIV') then 2

                    when regexp_contains(ziehl_res, r'PRESEN[ÇC]A.*B\.A\.A\.R') then 4
                    else null
                end
        end as id_resultado,

        loaded_at
    from base
),

exames_dedup as (
    select
        codigo_amostra,
        paciente_cpf,
        paciente_cns,
        nome,
        cnes,
        id_tipo_exame,
        id_resultado,
        dt_resultado,
        row_number() over (
            partition by paciente_cns, id_tipo_exame, dt_resultado
            order by loaded_at desc
        ) as rn
    from classificado
),

exames_filtrados as (
    select
        codigo_amostra,
        paciente_cpf,
        paciente_cns,
        nome,
        cnes,
        id_tipo_exame,
        id_resultado,
        dt_resultado
    from exames_dedup
    where dt_resultado  is not null
        and id_tipo_exame is not null
        and id_resultado  is not null
        and regexp_contains(paciente_cns, r'^\d{15}$')
        and paciente_cns <> '000000000000000'
        and rn = 1
),

notificacao as (
    select
        nu_cartao_sus,
        dt_notificacao,
        dt_encerramento,
        co_cid,
        tp_classificacao_final
    from {{ ref("mart_subpav_sinanrio__notificacao") }}
),

exames_com_notif as (
    select
        e.*,
        case
            when n.nu_cartao_sus is not null then 1
            else 0
        end as notificacao_ativa
    from exames_filtrados e
    left join notificacao n
        on e.paciente_cns = n.nu_cartao_sus
        and n.dt_notificacao <= e.dt_resultado
        and (n.dt_encerramento is null or n.dt_encerramento >= e.dt_resultado)
)

select
    codigo_amostra,
    paciente_cpf,
    paciente_cns,
    nome,
    cnes,
    id_tipo_exame,
    id_resultado,
    dt_resultado,
    notificacao_ativa,
    case
        when notificacao_ativa = 1 then 0  -- acompanhamento
        else 1                             -- diagnóstico
    end as diagnostico
from exames_com_notif
