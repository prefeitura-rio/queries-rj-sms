{{
    config(
        alias="paciente",
        materialized="incremental",
        unique_key="patient_cpf",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    events_from_window as (
        select *
        from {{ source("brutos_prontuario_vitacare_staging", "paciente_eventos") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),
    events_ranked_by_freshness as (
        select *, row_number() over (partition by patient_cpf order by source_updated_at desc) as rank
        from events_from_window
    ),
    latest_events as (select * from events_ranked_by_freshness where rank = 1)
select
    safe_cast(patient_cpf as string) as paciente_cpf,
    safe_cast(data__id as string) as id,
    safe_cast(data__cns as string) as cns,
    safe_cast(data__cpf as string) as cpf,
    safe_cast(data__dnv as string) as dnv,
    safe_cast(data__nis as string) as nis,
    safe_cast(data__nome as string) as nome,
    safe_cast(data__nomeSocial as string) as nome_social,
    safe_cast(data__nomeMae as string) as nome_mae,
    safe_cast(data__nomePai as string) as nome_pai,
    safe_cast(data__sexo as string) as sexo,
    safe_cast(data__nacionalidade as string) as nacionalidade,
    safe_cast(data__email as string) as email,
    safe_cast(data__obito as string) as obito,
    safe_cast(data__equipe as string) as equipe,
    safe_cast(data__racaCor as string) as raca_cor,
    safe_cast(data__telefone as string) as telefone,
    safe_cast(data__nPront as string) as numero_prontuario,
    safe_cast(data__dataNascimento as string) as data_nascimento,
    safe_cast(data__paisNascimento as string) as pais_nascimento,
    safe_cast(data__ap as string) as ap,
    safe_cast(data__unidade as string) as nome_unidade,
    safe_cast(data__cnes as string) as cnes_unidade,
    safe_cast(data__ineEquipe as string) as ine_equipe,
    safe_cast(data__codigoEquipe as string) as codigo_equipe,
    safe_cast(data__microarea as string) as microarea,
    safe_cast(data__cep as string) as cep,
    safe_cast(data__bairro as string) as bairro,
    safe_cast(data__logradouro as string) as logradouro,
    safe_cast(data__tipoLogradouro as string) as tipo_logradouro,
    safe_cast(data__dataCadastro as string) as data_cadastro,

    -- Ficha A
    safe_cast(data__ocupacao as string) as ocupacao,
    safe_cast(data__religiao as string) as religiao,
    safe_cast(data__escolaridade as string) as escolaridade,
    safe_cast(data__tempoMoradia as string) as tempo_moradia,
    safe_cast(data__tipoDomicilio as string) as tipo_domicilio,
    safe_cast(data__comodos as string) as comodos,
    safe_cast(data__destinoLixo as string) as destino_lixo,
    safe_cast(data__luzEletrica as string) as luz_eletrica,
    safe_cast(data__rendaFamiliar as string) as renda_familiar,
    safe_cast(data__tratamentoAgua as string) as tratamento_agua,
    safe_cast(data__emSituacaoDeRua as string) as em_situacao_de_rua,
    safe_cast(data__frequentaEscola as string) as frequenta_escola,
    safe_cast(data__meiosTransporte as string) as meios_transporte,
    safe_cast(data__situacaoUsuario as string) as situacao_usuario,
    safe_cast(data__doencasCondicoes as string) as doencas_condicoes,
    safe_cast(data__estadoNascimento as string) as estado_nascimento,
    safe_cast(data__estadoResidencia as string) as estado_residencia,
    safe_cast(data__identidadeGenero as string) as identidade_genero,
    safe_cast(data__meiosComunicacao as string) as meios_comunicacao,
    safe_cast(data__orientacaoSexual as string) as orientacao_sexual,
    safe_cast(data__possuiFiltroAgua as string) as possui_filtro_agua,
    safe_cast(data__possuiPlanoSaude as string) as possui_plano_saude,
    safe_cast(data__situacaoFamiliar as string) as situacao_familiar,
    safe_cast(data__territorioSocial as string) as territorio_social,
    safe_cast(data__abastecimentoAgua as string) as abastecimento_agua,
    safe_cast(data__animaisNoDomicilio as string) as animais_no_domicilio,
    safe_cast(data__cadastroPermanente as string) as cadastro_permanente,
    safe_cast(data__familiaLocalizacao as string) as familia_localizacao,
    safe_cast(data__emCasoDoencaProcura as string) as em_caso_doenca_procura,
    safe_cast(data__municipioNascimento as string) as municipio_nascimento,
    safe_cast(data__municipioResidencia as string) as municipio_residencia,
    safe_cast(data__responsavelFamiliar as string) as responsavel_familiar,
    safe_cast(data__esgotamentoSanitario as string) as esgotamento_sanitario,
    safe_cast(data__situacaoMoradiaPosse as string) as situacao_moradia_posse,
    safe_cast(data__situacaoProfissional as string) as situacao_profissional,
    safe_cast(data__vulnerabilidadeSocial as string) as vulnerabilidade_social,
    safe_cast(data__familiaBeneficiariaCfc as string) as familia_beneficiaria_cfc,
    safe_cast(data__dataAtualizacaoCadastro as string) as data_atualizacao_cadastro,
    safe_cast(data__participaGrupoComunitario as string) as participa_grupo_comunitario,
    safe_cast(data__relacaoResponsavelFamiliar as string) as relacao_responsavel_familiar,
    safe_cast(data__membroComunidadeTradicional as string) as membro_comunidade_tradicional,
    safe_cast(data__dataAtualizacaoVinculoEquipe as string) as data_atualizacao_vinculo_equipe,
    safe_cast(data__familiaBeneficiariaAuxilioBrasil as string) as familia_beneficiaria_auxilio_brasil,
    safe_cast(data__criancaMatriculadaCrechePreEscola as string) as crianca_matriculada_creche_pre_escola,

    safe_cast(source_updated_at as string) as updated_at,
    safe_cast(data_particao as date) as data_particao
from latest_events

