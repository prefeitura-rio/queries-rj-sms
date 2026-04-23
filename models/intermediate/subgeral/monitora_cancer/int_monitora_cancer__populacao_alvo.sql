-- noqa: disable=LT08

-- População-alvo do monitoramento de câncer de mama: pacientes com pelo menos
-- um evento de suspeita ou diagnóstico a partir de 2025-01-01, enriquecidos
-- com dados cadastrais (nome, idade, raça/cor, telefone) e de vínculo APS
-- (clínica, equipe, telefones institucionais).
-- Granularidade: 1 linha por paciente_cpf.

with
    populacao_interesse as (
        select
            paciente_cpf,
            case
                when max(case when sistema_origem = 'SER' then 1 else 0 end) = 1 then 'UNACON'
                when max(cast(criterio_diagnostico as int64)) = 1 then 'DIAGNOSTICO'
                else 'SUSPEITA'
            end as status
        from {{ ref("mart_monitora_cancer__fatos") }}
        where
            data_solicitacao >= "2025-01-01"
            and (
                criterio_suspeita = true
                or criterio_diagnostico = true
            )
        group by paciente_cpf
    )

select
    pop.paciente_cpf,
    pop.status,
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
    -- registro do HCI — elimina o desalinhamento do SAFE_OFFSET(0) anterior entre
    -- arrays paralelos.
dim_paciente.clinica_sf as clinica_sf,
dim_paciente.clinica_sf_ap as clinica_sf_ap,
dim_paciente.clinica_sf_telefone as clinica_sf_telefone,
dim_paciente.equipe_sf as equipe_sf,
dim_paciente.equipe_sf_telefone as equipe_sf_telefone

from populacao_interesse as pop

    left join {{ref("pacientes_subgeral__dim_paciente")}} as dim_paciente
    on pop.paciente_cpf = dim_paciente.cpf_particao

    left join {{ref("mart_iplanrio__telefones_validos")}} as telefones
    on pop.paciente_cpf = safe_cast(telefones.cpf as int)

    left join {{ref("raw_bcadastro__cpf")}} as bcadastro
    on pop.paciente_cpf = bcadastro.cpf_particao

where bcadastro.sexo != "masculino"
    and bcadastro.obito_ano is null
    and not exists(
        select 1
        from unnest (dim_paciente.anos_obito) as ano
        where ano is not null
    )
