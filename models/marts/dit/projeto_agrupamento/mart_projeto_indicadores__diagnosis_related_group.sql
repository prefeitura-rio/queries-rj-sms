{{
    config(
        alias="drg"
    )
}}

-- =============================================================================
-- DRG - Diagnosis Related Group
-- Hospital: CNES 2298120
-- Fonte: Prontuário Vitai
-- Escopo: Episódios de internação apenas (boletins com internacao_data preenchida)
-- Período: 2024-09-01 a 2026-09-01
--
-- Campos com incerteza estão comentados no código.
-- =============================================================================

with

constantes as (
    select
        date('2024-09-01') as data_minima,
        date('2026-09-01') as data_maxima,
        '2298120'          as id_cnes_hospital
),

-- ---------------------------------------------------------------------------
-- Estabelecimento: filtra apenas o hospital alvo via tabela mestra do Vitai
-- ---------------------------------------------------------------------------
estabelecimento as (
    select
        gid,
        cnes,
        nome_estabelecimento
    from {{ ref('raw_prontuario_vitai__m_estabelecimento') }}
    where cnes = (select id_cnes_hospital from constantes)
    qualify row_number() over (partition by cnes order by updated_at desc) = 1
),

-- ---------------------------------------------------------------------------
-- Boletim: hub central dos episódios - filtrado por estabelecimento e período
-- Incluímos apenas episódios com internação (internacao_data IS NOT NULL)
-- ---------------------------------------------------------------------------
boletim as (
    select
        b.gid,
        b.gid_paciente,
        b.gid_estabelecimento,
        e.cnes,
        b.numero_be,
        b.internacao_data,
        b.alta_data,
        b.cpf,
        b.cns
    from {{ ref('raw_prontuario_vitai__boletim') }} b
    inner join estabelecimento e on e.gid = b.gid_estabelecimento
    cross join constantes
    where
        b.internacao_data is not null
        and date(b.internacao_data)
            between (select data_minima from constantes)
                and (select data_maxima from constantes)
        and (b.cancelado is null or lower(b.cancelado) != 'true')
),

-- ---------------------------------------------------------------------------
-- Paciente: dados demográficos para idade, sexo e peso ao nascer (RN)
-- ---------------------------------------------------------------------------
paciente as (
    select
        gid,
        cpf,
        data_nascimento,
        sexo,
        numero_prontuario
    from {{ ref('raw_prontuario_vitai__paciente') }}
    qualify row_number() over (partition by gid order by updated_at desc) = 1
),

-- ---------------------------------------------------------------------------
-- Internação: procedimento SUS e diagnóstico principal da internação
-- ---------------------------------------------------------------------------
internacao as (
    select
        i.gid_boletim,
        i.id_procedimento,        -- código SUS do procedimento de internação
        i.id_diagnostico,         -- CID principal informado na internação
        i.gid_profissional,       -- FK para o médico responsável pela internação
        i.saida_data,
        i.internacao_data
    from {{ ref('raw_prontuario_vitai__internacao') }} i
    inner join boletim b on b.gid = i.gid_boletim
    qualify row_number() over (partition by i.gid_boletim order by i.internacao_data desc) = 1
),

-- ---------------------------------------------------------------------------
-- Resumo de Alta: CID da alta, desfecho da internação (Óbito, Alta, Transfer.)
-- Este é o CID mais confiável para DRG pois é o diagnóstico final.
-- ---------------------------------------------------------------------------
resumo_alta as (
    select
        ra.gid_boletim,
        ra.cid_codigo_alta,
        ra.cid_descricao_alta,
        ra.desfecho_internacao,
        ra.alta_tipo,
        ra.resumo_alta_datahora
    from {{ ref('raw_prontuario_vitai__resumo_alta') }} ra
    inner join boletim b on b.gid = ra.gid_boletim
    where ra.exclusao_alta_datahora is null  -- exclui altas canceladas
    qualify row_number() over (partition by ra.gid_boletim order by ra.resumo_alta_datahora desc) = 1
),

-- ---------------------------------------------------------------------------
-- Alta administrativa: tipo detalhado de alta para mapeamento de status
-- ---------------------------------------------------------------------------
alta as (
    select
        a.gid_boletim,
        a.alta_tipo,
        a.alta_tipo_detalhado,
        a.alta_data,
        a.abe_obs
    from {{ ref('raw_prontuario_vitai__alta') }} a
    inner join boletim b on b.gid = a.gid_boletim
    qualify row_number() over (partition by a.gid_boletim order by a.alta_administrativa_data desc) = 1
),

-- ---------------------------------------------------------------------------
-- Diagnósticos: todos os CIDs registrados no episódio (para CID secundário)
-- Exclui o diagnóstico principal (que vem do resumo_alta) para compor secundários
-- ---------------------------------------------------------------------------
diagnosticos as (
    select
        d.gid_boletim,
        d.codigo,
        d.descricao,
        d.tipo,
        d.situacao,
        row_number() over (
            partition by d.gid_boletim
            order by
                -- Prioriza diagnósticos definitivos e mais recentes
                case when lower(d.tipo) in ('definitivo', 'principal') then 0 else 1 end,
                d.data desc
        ) as rank_diagnostico
    from {{ ref('raw_prontuario_vitai__diagnostico') }} d
    inner join boletim b on b.gid = d.gid_boletim
    where d.situacao is null or lower(d.situacao) != 'cancelado'
),

-- Agrega diagnósticos secundários em string pipe-separated
-- e computa o indicador de presença na admissão por CID secundário:
--   S = CID secundário coincide com o CID_Principal efetivo do episódio
--   N = CID secundário NÃO coincide com o CID_Principal efetivo
--   U = nenhuma fonte de CID principal disponível para o episódio
-- Nota: W (clinicamente indeterminado) não é derivável automaticamente — fica como null.
-- O CID_Principal efetivo segue a mesma prioridade do SELECT final:
--   resumo_alta.cid_codigo_alta > internacao.id_diagnostico > cid_principal_fallback
-- A ordem dos indicadores respeita a mesma sequência de cid_secundarios.
diagnosticos_agregados as (
    select
        d.gid_boletim,
        -- CID de maior rank como fallback de principal (sem ponto)
        replace(max(case when d.rank_diagnostico = 1 then d.codigo end), '.', '') as cid_principal_fallback,
        -- CIDs secundários: todos exceto rank 1, sem ponto, sem "None", limitados a 10
        string_agg(
            case
                when d.rank_diagnostico > 1
                    and d.codigo is not null
                    and upper(d.codigo) != 'NONE'
                then replace(d.codigo, '.', '')
            end,
            '|'
            order by d.rank_diagnostico
            limit 10
        ) as cid_secundarios,
        -- Indicador de presença na admissão, na mesma sequência dos CIDs secundários.
        -- Compara contra o CID_Principal efetivo (mesma prioridade do SELECT final),
        -- sem ponto nos dois lados para uniformidade.
        string_agg(
            case
                when d.rank_diagnostico > 1
                    and d.codigo is not null
                    and upper(d.codigo) != 'NONE'
                then case
                    when coalesce(ra.cid_codigo_alta, i.id_diagnostico) is null then 'U'
                    when replace(d.codigo, '.', '') =
                         replace(coalesce(ra.cid_codigo_alta, i.id_diagnostico), '.', '') then 'S'
                    else 'N'
                end
            end,
            '|'
            order by d.rank_diagnostico
            limit 10
        ) as presenca_cid_secundario_admissao
    from diagnosticos d
    left join internacao   i  on i.gid_boletim  = d.gid_boletim
    left join resumo_alta  ra on ra.gid_boletim = d.gid_boletim
    group by d.gid_boletim
),

-- ---------------------------------------------------------------------------
-- Cirurgias: procedimentos cirúrgicos SUS realizados na internação
-- ---------------------------------------------------------------------------
cirurgias as (
    select
        c.gid_boletim,
        string_agg(
            c.procedimento_codigo,
            '|'
            order by c.cirurgia_data
        ) as codigos_cirurgia
    from {{ ref('raw_prontuario_vitai__cirurgia') }} c
    inner join boletim b on b.gid = c.gid_boletim
    where c.procedimento_codigo is not null
      and upper(c.procedimento_codigo) != 'NONE'
    group by c.gid_boletim
),

-- ---------------------------------------------------------------------------
-- Exames: procedimentos SUS de exames solicitados na internação
-- ---------------------------------------------------------------------------
exames as (
    select
        e.gid_boletim,
        string_agg(
            e.procedimento_codigo,
            '|'
            order by e.pedido_data
        ) as codigos_exame
    from {{ ref('raw_prontuario_vitai__exame') }} e
    inner join boletim b on b.gid = e.gid_boletim
    where e.procedimento_codigo is not null
      and upper(e.procedimento_codigo) != 'NONE'
    group by e.gid_boletim
),

-- ---------------------------------------------------------------------------
-- Recém-Nascido: peso ao nascer para pacientes com < 1 ano
-- Vinculado ao boletim via gid_boletim
-- ---------------------------------------------------------------------------
recem_nascido as (
    select
        rn.gid_boletim,
        rn.peso   -- peso em gramas ao nascer
    from {{ ref('raw_prontuario_vitai__dtw__recem_nascido') }} rn
    inner join boletim b on b.gid = rn.gid_boletim
    where rn.peso between 150 and 9000  -- faixa válida conforme dicionário DRG
    qualify row_number() over (partition by rn.gid_boletim order by rn.parto_datahora desc) = 1
),

-- ---------------------------------------------------------------------------
-- Numeração sequencial de pacientes, internações e médicos
-- Codigo_Paciente: número sequencial único por gid_paciente (denso, estável
--   dentro do período — ordenado pela primeira internação do paciente)
-- Codigo_Internacao: sequência de internações de um paciente, ordenada por
--   internacao_data (1ª internação = 1, 2ª = 2, etc.)
-- Codigo_Medico_Responsavel: número sequencial único por gid_profissional
--   (denso, estável — ordenado pela primeira internação atendida pelo médico)
-- ---------------------------------------------------------------------------

-- Passo 1: pré-calcula a primeira internação por paciente (necessário porque
-- o BigQuery não permite window function dentro de ORDER BY de outra window function)
primeira_internacao_por_paciente as (
    select
        gid_paciente,
        min(internacao_data) as primeira_internacao_data
    from boletim
    group by gid_paciente
),

-- Passo 2: pré-calcula a primeira internação atendida por cada médico
primeira_internacao_por_medico as (
    select
        i.gid_profissional,
        min(b.internacao_data) as primeira_internacao_data
    from internacao i
    inner join boletim b on b.gid = i.gid_boletim
    where i.gid_profissional is not null
    group by i.gid_profissional
),

sequencias as (
    select
        b.gid                                                          as gid_boletim,
        -- Número único por paciente: dense_rank pela primeira internação do paciente
        dense_rank() over (
            order by
                pip.primeira_internacao_data,
                b.gid_paciente  -- desempate determinístico
        )                                                              as Codigo_Paciente,
        -- Sequência de internações dentro do paciente, ordem cronológica
        row_number() over (
            partition by b.gid_paciente
            order by b.internacao_data, b.gid  -- desempate determinístico por gid
        )                                                              as Codigo_Internacao,
        -- Número único por médico responsável: dense_rank pela primeira internação atendida
        -- Médicos sem gid_profissional (internações sem médico vinculado) recebem null
        case
            when i.gid_profissional is not null
            then dense_rank() over (
                order by
                    pim.primeira_internacao_data,
                    i.gid_profissional  -- desempate determinístico
            )
        end                                                            as Codigo_Medico_Responsavel
    from boletim b
    inner join primeira_internacao_por_paciente pip
        on pip.gid_paciente = b.gid_paciente
    left join internacao i
        on i.gid_boletim = b.gid
    left join primeira_internacao_por_medico pim
        on pim.gid_profissional = i.gid_profissional
),

-- ---------------------------------------------------------------------------
-- Consolidação final do modelo
-- ---------------------------------------------------------------------------
internacoes as (
    select
        -- ── Hospital ──────────────────────────────────────────────────────────
        e.nome_estabelecimento                             as Hospital,

        right(e.cnes, 4)                                   as Codigo_Hospital,

        -- ── Paciente e Internação ─────────────────────────────────────────────
        -- Sequenciais densos e consistentes: mesmo paciente → mesmo código;
        -- internações de um paciente numeradas cronologicamente a partir de 1.
        seq.Codigo_Paciente                                as Codigo_Paciente,
        seq.Codigo_Internacao                              as Codigo_Internacao,

        -- ── Datas ─────────────────────────────────────────────────────────────
        format_date('%d/%m/%Y', date(b.internacao_data))     as Dt_Admissao,

        -- Data de saída: prioriza resumo_alta > internação.saida_data > boletim.alta_data
        format_date(
            '%d/%m/%Y',
            coalesce(
                date(ra.resumo_alta_datahora),
                date(i.saida_data),
                date(b.alta_data)
            )
        )                                                   as Dt_Saida,

        -- ── Idade ─────────────────────────────────────────────────────────────
        -- Calculada na data de saída (conforme padrão DRG)
        case
            when date_diff(
                coalesce(date(ra.resumo_alta_datahora), date(i.saida_data), date(b.alta_data)),
                p.data_nascimento, year
            ) >= 1
            then date_diff(
                coalesce(date(ra.resumo_alta_datahora), date(i.saida_data), date(b.alta_data)),
                p.data_nascimento, year
            )
            else 0
        end                                                 as Idade_Anos,

        case
            when date_diff(
                coalesce(date(ra.resumo_alta_datahora), date(i.saida_data), date(b.alta_data)),
                p.data_nascimento, year
            ) < 1
            then date_diff(
                coalesce(date(ra.resumo_alta_datahora), date(i.saida_data), date(b.alta_data)),
                p.data_nascimento, day
            )
            else null
        end                                                 as Idade_Dias,

        -- ── Peso ao Nascer ────────────────────────────────────────────────────
        -- Disponível apenas para RNs via tabela dtw__recem_nascido.
        safe_cast(rn.peso as int64)                        as Peso_ao_Nascer,

        -- ── Sexo ──────────────────────────────────────────────────────────────
        -- DRG: 1=Masculino, 2=Feminino
        case
            when upper(p.sexo) in ('M', 'MASC', 'MASCULINO', '1') then '1'
            when upper(p.sexo) in ('F', 'FEM', 'FEMININO', '2')   then '2'
            else null
        end                                                 as Sexo,

        -- ── Status da Alta ────────────────────────────────────────────────────
        case
            when trim(ra.desfecho_internacao) = 'ÓBITO'          then '20'
            when trim(ra.desfecho_internacao) = 'TRANSFERÊNCIA'  then '02'
            when trim(ra.desfecho_internacao) = 'ENCAMINHAMENTO' then '02'
            when trim(ra.desfecho_internacao) = 'EVASÃO'         then '07'
            when trim(ra.desfecho_internacao) = 'ALTA CLÍNICA'   then '01'
            -- Fallback: alta_tipo da tabela alta (valores ainda não validados)
            when upper(alt.alta_tipo) like '%ÓBITO%'             then '20'
            when upper(alt.alta_tipo) like '%OBITO%'             then '20'
            when upper(alt.alta_tipo) like '%TRANSFER%'          then '02'
            when upper(alt.alta_tipo) like '%EVASÃO%'            then '07'
            when upper(alt.alta_tipo) like '%EVASAO%'            then '07'
            when upper(alt.alta_tipo) like '%ALTA%'              then '01'
            else null
        end                                                 as Status_Alta,

        -- ── Ventilação Mecânica ───────────────────────────────────────────────
        null                                                as DVM,

        -- ── CID Principal ─────────────────────────────────────────────────────
        -- Prioridade: CID do resumo de alta (mais confiável) > CID da internação
        -- > CID com maior rank na tabela de diagnósticos.
        -- Ponto removido para uniformidade com CIDs secundários.
        replace(
            coalesce(
                ra.cid_codigo_alta,
                i.id_diagnostico,
                da.cid_principal_fallback
            ),
            '.', ''
        )                                                   as CID_Principal,

        -- ── CIDs Secundários ──────────────────────────────────────────────────
        -- Todos os CIDs registrados exceto o principal, separados por " | "
        da.cid_secundarios                                  as CID_Secundario,

        -- ── Presença do CID Secundário na Admissão ────────────────────────────
        -- Indica por CID secundário se estava presente na admissão:
        --   S = coincide com o CID de admissão (internacao.id_diagnostico)
        --   N = não coincide com o CID de admissão
        --   U = CID de admissão não foi registrado (id_diagnostico IS NULL)
        --   W = clinicamente indeterminado (não derivável automaticamente — null)
        -- Separador: "|", na mesma sequência de CID_Secundario.
        da.presenca_cid_secundario_admissao                 as Presenca_CID_Secundario_Admissao,

        -- ── Procedimentos SUS ─────────────────────────────────────────────────
        -- Consolida: procedimento da internação + cirurgias + exames com código SUS
        -- Separador: " | "
        trim(
            concat(
                coalesce(i.id_procedimento, ''),
                case when i.id_procedimento is not null and (cir.codigos_cirurgia is not null or ex.codigos_exame is not null) then '|' else '' end,
                coalesce(cir.codigos_cirurgia, ''),
                case when cir.codigos_cirurgia is not null and ex.codigos_exame is not null then '|' else '' end,
                coalesce(ex.codigos_exame, '')
            )
        )                                                   as Codigos_Procedimento_SUS,

        -- Tabela de procedimentos: sempre SUS (3) neste contexto
        '3'                                                 as Tipo_Tabela,
        'SUS'                                               as Fonte_Pagadora,

        -- ── Código do Médico Responsável ──────────────────────────────────────
        seq.Codigo_Medico_Responsavel                       as Codigo_Medico_Responsavel

    from boletim b
    inner join estabelecimento e          on e.gid = b.gid_estabelecimento
    inner join sequencias seq             on seq.gid_boletim = b.gid
    left  join paciente p                 on p.gid = b.gid_paciente
    left  join internacao i               on i.gid_boletim = b.gid
    left  join resumo_alta ra             on ra.gid_boletim = b.gid
    left  join alta alt                   on alt.gid_boletim = b.gid
    left  join diagnosticos_agregados da  on da.gid_boletim = b.gid
    left  join cirurgias cir              on cir.gid_boletim = b.gid
    left  join exames ex                  on ex.gid_boletim = b.gid
    left  join recem_nascido rn           on rn.gid_boletim = b.gid
),

com_dados_basicos as (
    select *
    from internacoes
    where 
        (Sexo is not null) and
        (Idade_Anos is not null or Idade_Dias is not null) and
        (Codigo_Medico_Responsavel is not null)
)

select *
from com_dados_basicos
