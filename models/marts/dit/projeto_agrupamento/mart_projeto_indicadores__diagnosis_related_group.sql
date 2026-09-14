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
diagnosticos_agregados as (
    select
        gid_boletim,
        -- CID de maior rank para referenciar como "principal" (fallback se resumo_alta não tiver)
        max(case when rank_diagnostico = 1 then codigo end) as cid_principal_fallback,
        -- CIDs secundários: todos exceto rank 1, limitados a 10
        string_agg(
            case when rank_diagnostico > 1 then codigo end,
            ' | '
            order by rank_diagnostico
            limit 10
        ) as cid_secundarios
    from diagnosticos
    group by gid_boletim
),

-- ---------------------------------------------------------------------------
-- Cirurgias: procedimentos cirúrgicos SUS realizados na internação
-- ---------------------------------------------------------------------------
cirurgias as (
    select
        c.gid_boletim,
        string_agg(
            c.procedimento_codigo,
            ' | '
            order by c.cirurgia_data
        ) as codigos_cirurgia
    from {{ ref('raw_prontuario_vitai__cirurgia') }} c
    inner join boletim b on b.gid = c.gid_boletim
    where c.procedimento_codigo is not null
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
            ' | '
            order by e.pedido_data
        ) as codigos_exame
    from {{ ref('raw_prontuario_vitai__exame') }} e
    inner join boletim b on b.gid = e.gid_boletim
    where e.procedimento_codigo is not null
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
-- Consolidação final do modelo
-- ---------------------------------------------------------------------------
internacoes as (
    select
        b.gid                                              as gid_boletim,

        -- ── Hospital ──────────────────────────────────────────────────────────
        e.nome_estabelecimento                             as Hospital,

        -- INCERTEZA: O campo "Código do Hospital" do DRG é um código de 4 chars
        -- definido pelo fornecedor do sistema de agrupamento, não o CNES.
        -- Deve ser obtido diretamente com o fornecedor do DRG.
        right(e.cnes, 4)                                   as Codigo_Hospital,

        -- ── Paciente ──────────────────────────────────────────────────────────
        -- Usando os últimos 6 chars do gid_paciente como identificador interno
        right(b.gid_paciente, 6)                           as Codigo_Paciente,

        -- Código da internação = últimos 2 chars do gid do boletim
        -- INCERTEZA: O dicionário indica 2 chars mas isso parece muito pequeno
        -- para um identificador único; pode ser necessais caracteres.
        right(b.gid, 2)                                    as Codigo_Internacao,

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
        -- INCERTEZA: Nem todos os RNs terão registro nessa tabela.
        safe_cast(rn.peso as int64)                        as Peso_ao_Nascer,

        -- ── Sexo ──────────────────────────────────────────────────────────────
        -- DRG: 1=Masculino, 2=Feminino
        -- INCERTEZA: valores exatos no Vitai podem variar (ex: 'M'/'F', 'MASCULINO', etc.)
        case
            when upper(p.sexo) in ('M', 'MASC', 'MASCULINO', '1') then '1'
            when upper(p.sexo) in ('F', 'FEM', 'FEMININO', '2')   then '2'
            else null  -- sexo desconhecido ou não binário: verificar padrão real do Vitai
        end                                                 as Sexo,

        -- ── Status da Alta ────────────────────────────────────────────────────
        -- Mapeamento: desfecho_internacao e alta_tipo -> código DRG
        -- INCERTEZA: Os valores exatos dos campos tipo_alta e desfecho_internacao
        -- no Vitai precisam ser validados com os dados reais.
        case
            when upper(coalesce(ra.desfecho_internacao, alt.alta_tipo)) like '%ÓBITO%'        then '20'
            when upper(coalesce(ra.desfecho_internacao, alt.alta_tipo)) like '%OBITO%'        then '20'
            when upper(coalesce(ra.desfecho_internacao, alt.alta_tipo)) like '%TRANSFER%'     then '02'
            when upper(coalesce(ra.desfecho_internacao, alt.alta_tipo)) like '%EVASÃO%'       then '07'
            when upper(coalesce(ra.desfecho_internacao, alt.alta_tipo)) like '%EVASAO%'       then '07'
            when upper(coalesce(ra.desfecho_internacao, alt.alta_tipo)) like '%ALTA%'         then '01'
            else null  -- status de alta não mapeado; verificar valores reais
        end                                                 as Status_Alta,

        -- ── Ventilação Mecânica ───────────────────────────────────────────────
        -- INCERTEZA: Não há campo explícito de ventilação mecânica no PEP
        null                                                as DVM,

        -- ── CID Principal ─────────────────────────────────────────────────────
        -- Prioridade: CID do resumo de alta (mais confiável) > CID da internação
        -- > CID com maior rank na tabela de diagnósticos
        coalesce(
            ra.cid_codigo_alta,
            i.id_diagnostico,
            da.cid_principal_fallback
        )                                                   as CID_Principal,

        -- ── CIDs Secundários ──────────────────────────────────────────────────
        -- Todos os CIDs registrados exceto o principal, separados por " | "
        da.cid_secundarios                                  as CID_Secundario,

        -- ── Procedimentos SUS ─────────────────────────────────────────────────
        -- Consolida: procedimento da internação + cirurgias + exames com código SUS
        -- Separador: " | "
        trim(
            concat(
                coalesce(i.id_procedimento, ''),
                case when i.id_procedimento is not null and (cir.codigos_cirurgia is not null or ex.codigos_exame is not null) then ' | ' else '' end,
                coalesce(cir.codigos_cirurgia, ''),
                case when cir.codigos_cirurgia is not null and ex.codigos_exame is not null then ' | ' else '' end,
                coalesce(ex.codigos_exame, '')
            )
        )                                                   as Codigos_Procedimento_SUS,

        -- Tabela de procedimentos: sempre SUS (3) neste contexto
        '3'                                                 as Tipo_Tabela

    from boletim b
    inner join estabelecimento e          on e.gid = b.gid_estabelecimento
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
        Sexo is not null and
        (Idade_Anos is not null or Idade_Dias is not null)
)

select *
from com_dados_basicos
