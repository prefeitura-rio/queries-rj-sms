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
            when tipo_exame = 'tubb-colzn'  then 1
            when tipo_exame = 'tugexp-pcrtr' then 2
            when tipo_exame = 'tubc-culmb' then 3
        end as id_tipo_exame,

        coalesce(safe.parse_date('%d/%m/%Y', data_liberacao), safe.parse_date('%Y-%m-%d', data_liberacao), safe.parse_date('%d-%m-%Y', data_liberacao)) as dt_resultado,
        coalesce(safe.parse_date('%d/%m/%Y', data_coleta), safe.parse_date('%Y-%m-%d', data_coleta), safe.parse_date('%d-%m-%Y', data_coleta)) as dt_coleta,
        coalesce(safe.parse_date('%d/%m/%Y', data_liberacao), safe.parse_date('%Y-%m-%d', data_liberacao), safe.parse_date('%d-%m-%Y', data_liberacao)) as dt_liberacao,
        coalesce(safe.parse_date('%d/%m/%Y', data_processamento), safe.parse_date('%Y-%m-%d', data_processamento), safe.parse_date('%d-%m-%Y', data_processamento)) as dt_processamento,
        coalesce(safe.parse_date('%d/%m/%Y', data_inicio_processamento), safe.parse_date('%Y-%m-%d', data_inicio_processamento), safe.parse_date('%d-%m-%Y', data_inicio_processamento)) as dt_inicio_processamento,
        coalesce(safe.parse_date('%d/%m/%Y', data_recebimento), safe.parse_date('%Y-%m-%d', data_recebimento), safe.parse_date('%d-%m-%Y', data_recebimento)) as dt_recebimento,
        coalesce(safe.parse_date('%d/%m/%Y', data_solicitacao), safe.parse_date('%Y-%m-%d', data_solicitacao), safe.parse_date('%d-%m-%Y', data_solicitacao)) as dt_solicitacao,

        upper(tugexp_pcrtr.dna_para_complexo_mycobacterium_tuberculosis) as pcr_dna,
        upper(tugexp_pcrtr.rifampicina)                                  as pcr_rif,
        upper(tubb_colzn.resultado)                                      as ziehl_res,
        upper(tubc_culmb.resultado)                                      as cultura_res,

        loaded_at
    from {{ ref('raw_gal__exames_laboratoriais') }}
    where tipo_exame in ('tugexp-pcrtr','tubb-colzn','tubc-culmb')
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
        dt_coleta,
        dt_liberacao,
        dt_processamento,
        dt_inicio_processamento,
        dt_recebimento,
        dt_solicitacao,        
        case
            when id_tipo_exame = 1 then
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

            when id_tipo_exame = 2 then
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

            when id_tipo_exame = 3 then
                case
                    when cultura_res is null or trim(cultura_res) = '' then 3

                    when regexp_contains(cultura_res, r'\bN/?RE\b')
                    or regexp_contains(cultura_res, r'N[ÃA]O\s*REALIZ') then 4

                    when regexp_contains(cultura_res, r'^POSITIV') then 1

                    when cultura_res = 'NEGATIVA'
                    or regexp_contains(cultura_res, r'^CONTAMIN')
                    or regexp_contains(cultura_res, r'^MICOBAC')
                    then 2

                    when regexp_contains(cultura_res, r'EM\s+ANDAMENTO') then 3

                    else 3
                end
        end as id_resultado,
        case
            when id_tipo_exame = 1 then ziehl_res
            when id_tipo_exame = 2 then trim(
                concat(
                ifnull(pcr_dna, ''),
                case
                    when pcr_rif is not null and trim(pcr_rif) <> '' then concat(' / RIF: ', pcr_rif)
                    else ''
                end
                )
            )
            when id_tipo_exame = 3 then cultura_res
            end as resultado_texto,
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
        resultado_texto,
        dt_coleta,
        dt_liberacao,
        dt_processamento,
        dt_inicio_processamento,
        dt_recebimento,
        dt_solicitacao,
        row_number() over (
            partition by paciente_cns, id_tipo_exame, dt_resultado, coalesce(codigo_amostra, '')
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
        dt_resultado,
        resultado_texto,
        dt_coleta,
        dt_liberacao,
        dt_processamento,
        dt_inicio_processamento,
        dt_recebimento,
        dt_solicitacao
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
        regexp_replace(nu_cartao_sus, r'\D', '') as nu_cartao_sus,
        nu_notificacao,
        dt_notificacao as dt_diagnostico,
        dt_encerramento,
        co_cid,
        tp_classificacao_final
    from {{ ref("mart_subpav_sinanrio__notificacao") }}
    where regexp_contains(regexp_replace(nu_cartao_sus, r'\D', ''), r'^\d{15}$')
),

exames_com_notif as (
    select
        e.*,
        case
            when n.nu_cartao_sus is not null then 1
            else 0
        end as notificacao_ativa,
        n.nu_notificacao,
        n.dt_diagnostico
    from exames_filtrados e
    left join notificacao n
        on e.paciente_cns = n.nu_cartao_sus
        and e.dt_coleta is not null
        and date_diff(e.dt_coleta, n.dt_diagnostico, day) between 0 and 30
    qualify row_number() over (
        partition by e.paciente_cns, e.id_tipo_exame, e.dt_resultado, e.codigo_amostra
        order by
            abs(date_diff(n.dt_diagnostico, e.dt_coleta, day)) asc,
            n.dt_diagnostico desc
    ) = 1
),

sintomatico_por_cns as (
    select
        regexp_replace(cns, r'\D', '') as cns,
        id_sintomatico
    from {{ ref('raw_plataforma_subpav_sinanrio__tb_sintomatico') }}
    where regexp_contains(regexp_replace(cns, r'\D', ''), r'^\d{15}$')
    qualify row_number() over (
        partition by regexp_replace(cns, r'\D', '')
        order by created_at desc, datalake_loaded_at desc, id_sintomatico desc
    ) = 1
),

sintomatico_por_cpf as (
    select
        regexp_replace(cpf, r'\D', '') as cpf,
        id_sintomatico
    from {{ ref('raw_plataforma_subpav_sinanrio__tb_sintomatico') }}
    where regexp_contains(regexp_replace(cpf, r'\D', ''), r'^\d{11}$')
    qualify row_number() over (
        partition by regexp_replace(cpf, r'\D', '')
        order by created_at desc, datalake_loaded_at desc, id_sintomatico desc
    ) = 1
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
    nu_notificacao as n_sinan,
    dt_diagnostico,
    notificacao_ativa,
    case
        when notificacao_ativa = 1 then 0  -- acompanhamento
        else 1                             -- diagnóstico
    end as diagnostico,
    resultado_texto,
    dt_coleta,
    dt_liberacao,
    dt_processamento,
    dt_inicio_processamento,
    dt_recebimento,
    dt_solicitacao,
    coalesce(scns.id_sintomatico, scpf.id_sintomatico) as id_sintomatico
from exames_com_notif e
left join sintomatico_por_cns scns
    on e.paciente_cns = scns.cns
left join sintomatico_por_cpf scpf
    on e.paciente_cpf = scpf.cpf
