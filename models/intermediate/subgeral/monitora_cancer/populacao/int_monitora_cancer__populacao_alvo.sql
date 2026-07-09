-- noqa: disable=LT08

-- População-alvo do monitoramento de câncer de mama: pacientes com pelo menos
-- um evento de suspeita ou diagnóstico a partir de 2025-01-01, enriquecidos
-- com dados cadastrais (nome, idade, raça/cor, telefone) e de vínculo APS
-- (clínica, equipe, telefones institucionais).
-- Granularidade: 1 linha por paciente_cpf.
--
-- Regras de exclusão aplicadas aqui:
--   (i) Óbito (cadastral): filtro direto via bcadastro.obito_ano IS NULL e
--       dim_paciente.anos_obito (fonte de verdade: bcadastro → HCI).
--   (ii)-(v) Exclusões derivadas do histórico de eventos (último evento
--       mamografia Cat 1/2, duas mamografias Cat 3, biópsia sem lesão, SER
--       antigo) são centralizadas em int_monitora_cancer__exclusoes e
--       aplicadas abaixo via anti-join. Consulte aquele modelo para a
--       documentação completa de cada regra.

with
    -- Conjunto de CPFs qualificados para o monitoramento. O status da paciente
    -- NÃO é decidido aqui: ele é derivado da jornada atual em
    -- int_monitora_cancer__eventos_episodios (onde o run_id existe).
    populacao_interesse as (
        select distinct paciente_cpf
        from {{ ref("mart_monitora_cancer__fatos") }}
        where
            data_solicitacao >= "2025-01-01"
            and (
                criterio_suspeita = true
                or criterio_diagnostico = true
            )
    ),

    gestantes_ativas as (
        select distinct cpf
        from {{ ref("mart_bi_gestacoes__gestacoes") }}
        where fase_atual = 'Gestação'
    )

select
    pop.paciente_cpf,
    bcadastro.nome,

    -- raça/cor escolhida pela regra de prioridade definida em pacientes_subgeral__dim_paciente:
    -- bcadastro > hci > minha_saude > resto, com fallback automático quando o vencedor é NULL.
    dim_paciente.raca_cor as raca_cor,

    -- DDI é opcional: substituído por '' para não propagar NULL no concat.
    -- DDD e número precisam estar presentes para montar o telefone.
    coalesce(
        if(
            trim(bcadastro.contato.telefone.ddd) is not null
            and trim(bcadastro.contato.telefone.numero) is not null,
            concat(
                ifnull(trim(bcadastro.contato.telefone.ddi), ''),
                trim(bcadastro.contato.telefone.ddd),
                trim(bcadastro.contato.telefone.numero)
            ),
            null
        ),
        telefones.`telefones` [SAFE_OFFSET(0) ].telefone_formatado
    ) as telefone,

    -- Idade médica: subtrai 1 se o aniversário ainda não ocorreu no ano corrente.
    -- date_diff com YEAR compara apenas o componente do ano (YEAR(a) - YEAR(b)),
    -- o que superestima a idade em até 1 ano antes do aniversário.
    date_diff(current_date('America/Sao_Paulo'), bcadastro.nascimento_data, year)
    - if(
    format_date('%m%d', current_date('America/Sao_Paulo'))
    < format_date('%m%d', bcadastro.nascimento_data),
    1,
    0
) as idade,

    -- vínculo APS sempre vem do HCI (cadastro consolidado da APS). pacientes que
    -- não estão no HCI ficam com esses campos NULL. todos os campos vêm do MESMO
    -- registro do HCI
    dim_paciente.clinica_sf as clinica_sf,
    dim_paciente.clinica_sf_ap as clinica_sf_ap,
    dim_paciente.clinica_sf_telefone as clinica_sf_telefone,
    dim_paciente.equipe_sf as equipe_sf,
    dim_paciente.equipe_sf_telefone as equipe_sf_telefone,

    gest.cpf is not null as gestante

from populacao_interesse as pop

left join {{ref("pacientes_subgeral__dim_paciente")}} as dim_paciente
on pop.paciente_cpf = dim_paciente.cpf_particao

left join {{ref("mart_iplanrio__telefones_validos")}} as telefones
on pop.paciente_cpf = safe_cast(telefones.cpf as int)

left join {{ref("raw_bcadastro__cpf")}} as bcadastro
on pop.paciente_cpf = bcadastro.cpf_particao

left join gestantes_ativas as gest
on lpad(safe_cast(pop.paciente_cpf as string), 11, '0') = gest.cpf

where bcadastro.sexo != "masculino"
    and bcadastro.obito_ano is null
    and not exists(
        select 1
        from unnest (dim_paciente.anos_obito) as ano
        where ano is not null
    )
    -- anti-join: o paciente permanece na população-alvo somente
    -- se não existir um registro correspondente em int_monitora_cancer__exclusoes
    and not exists(
        select 1
        from {{ ref("int_monitora_cancer__exclusoes") }} as excl
        where excl.paciente_cpf = pop.paciente_cpf
    )
