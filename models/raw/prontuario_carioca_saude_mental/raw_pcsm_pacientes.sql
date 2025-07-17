{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="pacientes",
        materialized="table",
        tags=["raw", "pcsm"],
        description="Pacientes que já obtiveram atendimento em um Centro de Atenção Psicosocial (CAPS) ou dentro da RAPS (Rede de atenção Psicosocial). Esta tabela tem informações complementares na tabela gh_paciente_sm."
    )
}} 

select
    safe_cast(cp.seqpac as int64) as id_paciente,
    safe_cast(cp.nuprontpapel as string) as numero_prontuario_papel,
    safe_cast(cp.dscnomepac as string) as nome_paciente,
    safe_cast(cp.dscnomsoci as string) as nome_social_paciente,
    safe_cast(cp.apelido as string) as apelido_paciente,
    safe_cast(cp.racacor as string) as raca_cor_paciente,
    case trim(safe_cast(cp.racacor as string))
        when '01' then 'Branca'
        when '02' then 'Preta'
        when '03' then 'Parda'
        when '04' then 'Amarela'
        when '05' then 'Indígena'
        when '00' then 'Não informado'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_raca_cor_paciente,
    safe_cast(cp.datnascim as date) as data_nascimento_paciente,
    safe_cast(cp.dscnmmae as string) as nome_mae_paciente,
    safe_cast(cp.dscnmpai as string) as nome_pai_paciente,
    safe_cast(cp.indsexo as string) as sexo_paciente,
    case trim(safe_cast(cp.indsexo as string))
        when 'M' then 'Masculino'
        when 'F' then 'Feminino'
        when 'I' then 'Não informado'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_sexo_paciente,
    safe_cast(cp.indgenero as string) as genero_paciente,
    case trim(safe_cast(cp.indgenero as string))
        when '01' then 'Mulher transgênero'
        when '02' then 'Travesti'
        when '03' then 'Homem transgênero'
        when '04' then 'Intersexo'
        when '05' then 'Mulher cisgênero'
        when '06' then 'Homem cisgênero'
        when '07' then 'Não binário'
        when '98' then 'Outro'
        when '99' then 'Não informado'
        when '00' then 'Não informado'
        when '' then 'Não informado'    
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_genero_paciente,   
    safe_cast(cp.sigufnasc as string) as sigla_uf_nascimento,
    safe_cast(cp.muninasc as string) as municipio_nascimento,
    safe_cast(cp.nacional as string) as nacionalidade_paciente,
    case trim(safe_cast(cp.nacional as string))
        when '1' then 'Brasileira'
        when '2' then 'Estrangeira'
        when '3' then 'Brasileiro naturalizado'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_nacionalidade_paciente,
    safe_cast(cp.paisorigem as string) as pais_origem_paciente,
    safe_cast(cp.dsctelef1 as string) as telefone1_paciente,
    safe_cast(cp.dsctelef2 as string) as telefone2_paciente,
    safe_cast(cp.dscemail as string) as email_paciente,
    safe_cast(cp.numrg as string) as numero_registro_civil,
    safe_cast(cp.indorgaoexp as string) as tipo_registro_civil,
    case trim(safe_cast(cp.indorgaoexp as string))
        when '10' then 'SSP'
        when '12' then 'OAB'
        when '20' then 'DIC'
        when '40' then 'Organismos militares'
        when '41' then 'Ministério da aeronáutica'
        when '42' then 'Ministério do exército'
        when '43' then 'Ministério da marinha'
        when '44' then 'Polícia federal'
        when '60' then 'Carteira de identidade classista'
        when '81' then 'Outros emissores'
        when '82' then 'Documento estrangeiro'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tipo_registro_civil,
    safe_cast(cp.datexpedi as date) as data_expedicao_registro,
    safe_cast(cp.ufexpedi as string) as sigla_uf_registro,
    safe_cast(cp.numcpfpac as string) as numero_cpf_paciente,
    safe_cast(cp.dsccns as string) as numero_cartao_saude,
    safe_cast(cp.estcivil as string) as estado_civil_paciente,
    case trim(safe_cast(cp.estcivil as string))
        when '1' then 'Solteiro(a)'
        when '2' then 'Casado(a)'
        when '3' then 'Viúvo(a)'
        when '4' then 'Separado(a) judicialmente'
        when '5' then 'União consensual'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_estado_civil_paciente,    
    safe_cast(cp.numcepender as string) as numero_cep_paciente,
    safe_cast(cp.dslogradouro as string) as logradouro_paciente,
    safe_cast(cp.dscbairroender as string) as bairro_paciente,
    safe_cast(cp.sigufender as string) as sigla_uf_endereco,
    safe_cast(cp.muniend as string) as municipio_endereco,
    safe_cast(cp.numend as string) as numero_endereco,
    safe_cast(cp.complend as string) as complemento_endereco,
    safe_cast(cp.pontorefe as string) as ponto_referencia_endereco,
    safe_cast(cp.dscfoto as string) as endereco_arquivo_foto_paciente,
    safe_cast(cp.indstatus as string) as status_acompanhamento,
    case trim(safe_cast(cp.indstatus as string))
        when '0' then 'Inativo'
        when '00' then 'Não catalogado'
        when '1' then 'Ativo'
        when '2' then 'Inativo'
        when '99' then 'Somente Cadastro'
        when 'A' then 'Busca ativa'
        when 'B' then 'Acompanhamento ambulatorio'
        when 'C' then 'Alta para CAPS de outro municipio'
        when 'D' then 'Desaparecido'
        when 'E' then 'Acompanhamento CAPS'
        when 'I' then 'Alta por insucesso de busca ativa'
        when 'M' then 'Alta por melhora'
        when 'O' then 'Óbito'
        when 'P' then 'Alta a pedido'
        when 'R' then 'Acompanhamento intersetorial'
        when 'S' then 'Alta para atenção básica'
        when 'T' then 'Transferência para outro municipio'
        when 'U' then 'Alta para outro ponto de atenção'
        when 'X' then 'Fechado pela unificação'
        when 'Z' then 'Finalização do programa Seguir em Frente'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_status_acompanhamento,
    safe_cast(cp.sequsref as int64) as id_unidade_atencao_primaria_referencia,
    safe_cast(cp.sequscapref as int64) as id_unidade_caps_referencia,
    safe_cast(cp.seqlogin as int64) as id_usuario_registrou_paciente,
    safe_cast(cp.datcadast as date) as data_cadastro_paciente,
    safe_cast(cp.seqprofequirefer1 as int64) as id_primeiro_profissional_referencia,
    safe_cast(cp.seqprofequirefer2 as int64) as id_segundo_profissional_referencia,
    safe_cast(cp.sequsambref as int64) as id_unidade_ambulatorial_referencia,
    safe_cast(cp.dscnotif as string) as notificacao_sistema,
    safe_cast(cp.indorigcad as string) as origem_cadastro,
    case trim(safe_cast(cp.indorigcad as string))
        when 'R' then 'RAPS'
        when 'I' then 'Intersetorial'
        when 'B' then 'Atenção básica'
        when 'F' then 'Programa Seguir em Frente'
        when 'N' then 'Não informado'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_origem_cadastro,
    safe_cast(cp.condestr as string) as status_paciente_nao_brasileiro,
    case trim(safe_cast(cp.condestr as string))
        when '1' then 'Apátrida'
        when '2' then 'Asilado'
        when '3' then 'Imigrante com autorização de residência'
        when '4' then 'Imigrante com visto temporário'
        when '5' then 'Refugiado (não legalizado)'
        when '6' then 'Refugiado (em solicitação)'
        when '7' then 'Refugiado legalizado'
        when '8' then 'Turista'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_status_paciente_nao_brasileiro,
    safe_cast(cp.dscmodatendativ as string) as ciclo_atendimento_aberto,
    case trim(safe_cast(cp.dscmodatendativ as string))
        when 'C' then 'Somente um ciclo de CAPS'
        when 'C,D' then 'Tem os dois'
        when 'D' then 'Somente um ciclo de deambulatório'
        when 'D,C' then 'Tem os dois'
        when '' then 'Não possui ciclo aberto'
        when null then 'Não possui ciclo aberto'
        else 'Não classificado'
    end as descricao_ciclo_atendimento_aberto,
    safe_cast(cp.seqprofrefdamb1 as int64) as id_primeiro_profissional_deambulatorial,
    safe_cast(cp.seqprofrefdamb2 as int64) as id_segundo_profissional_deambulatorial,
    safe_cast(cp.seqequipfm as int64) as id_equipe_clinica_familia,
    safe_cast(p_sm.indcobert as string) as moradia_coberta_saude_familia,
    case trim(safe_cast(p_sm.indcobert as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_moradia_coberta_saude_familia,
    safe_cast(p_sm.indcadesf as string) as cadastrado_saude_familia,
    case trim(safe_cast(p_sm.indcadesf as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_cadastrado_saude_familia,
    safe_cast(p_sm.indsitfam as string) as situacao_familiar,
    case trim(safe_cast(p_sm.indsitfam as string))
        when '01' then 'Convive c/companheira(o) e filho(s)'
        when '02' then 'Convive c/companheira(o), com laços conjugais e sem filhos'
        when '03' then 'Convive c/companheira(o), com filho(s) e/ou familiares'
        when '04' then 'Convive com familiar(es) sem companheira(o)'
        when '05' then 'Convive com outra(s) pessoa(s) sem laços consanguíneos e/ou conjugais'
        when '06' then 'Vive só'
        when '07' then 'Vive com familiares ou responsáveis'
        when '08' then 'Vive em situação de acolhimento institucional'
        when '99' then 'Sem informações'
        when '00' then 'Não informado'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_situacao_familiar, 
    safe_cast(p_sm.indmoradi as string) as tipo_moradia,
    case trim(safe_cast(p_sm.indmoradi as string))
        when '1' then 'Própria'
        when '2' then 'Alugada'
        when '3' then 'Cedida'
        when '4' then 'Unidades da assistência social'
        when '5' then 'Situação de rua'
        when '6' then 'SRT / República'
        when '7' then 'Pensionato'
        when '8' then 'Família acolhedora'
        when '9' then 'Instituição'
        when 'A' then 'Unidade de acolhimento adulto - UAA'
        when 'B' then 'Moradia assistida'
        when 'C' then 'Ocupação/Invasão'
        when 'D' then 'Moradia Programa Seguir em Frente'
        when 'E' then 'Desconhecido'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tipo_moradia,
    safe_cast(p_sm.localmora as string) as local_moradia, 
    safe_cast(p_sm.horariomora as string) as horario_moradia,
    case trim(safe_cast(p_sm.horariomora as string))
        when '1' then 'Diurno'
        when '2' then 'Noturno'
        when '1,2' then 'Diurno e Noturno'
        when '2,1' then 'Diurno e Noturno'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_horario_moradia,
    safe_cast(p_sm.obsmora as string) as obs_situacao_rua, 
    safe_cast(p_sm.ocupaend as string) as endereco_ocupacao,
    safe_cast(p_sm.ocupatel as string) as telefone_trabalho, 
    safe_cast(p_sm.subnmresp as string) as responsavel_paciente,
    safe_cast(p_sm.subtelresp as string) as telefone_responsavel,
    safe_cast(p_sm.subgrauparent as string) as parentesco_responsavel,
    safe_cast(p_sm.indtrab as string) as paciente_trabalhando,
    case trim(safe_cast(p_sm.indtrab as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_paciente_trabalhando,  
    safe_cast(p_sm.nmprofi as string) as profissao_paciente,
    safe_cast(p_sm.indescolari as string) as escolaridade_paciente,
    case trim(safe_cast(p_sm.indescolari as string))
        when '01' then 'Não sabe ler/escrever'
        when '02' then 'Alfabetizado'
        when '03' then 'Nível fundamental incompleto'
        when '04' then 'Nível fundamental completo'
        when '05' then 'Nível médio incompleto'
        when '06' then 'Nível médio completo'
        when '07' then 'Superior incompleto'
        when '08' then 'Superior completo'
        when '09' then 'Especialização'
        when '10' then 'Mestrado'
        when '11' then 'Doutorado'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_escolaridade_paciente,
    safe_cast(p_sm.indfreqescol as string) as frequenta_escola,
    case trim(safe_cast(p_sm.indfreqescol as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_frequenta_escola,
    safe_cast(p_sm.indserie as string) as serie_cursando,
    case trim(safe_cast(p_sm.indserie as string))
        when '1' then 'Infantil - G1'
        when '2' then 'Infantil - G2'
        when '3' then 'Infantil - G3'
        when '4' then 'Infantil - G4'
        when '5' then 'Infantil - G5'
        when '6' then 'Fundamental I - 1º Ano'
        when '7' then 'Fundamental I - 2º Ano'
        when '8' then 'Fundamental I - 3º Ano'
        when '9' then 'Fundamental I - 4º Ano'
        when '10' then 'Fundamental I - 5º Ano'
        when '11' then 'Fundamental II - 6º Ano'
        when '12' then 'Fundamental II - 7º Ano'
        when '13' then 'Fundamental II - 8º Ano'
        when '14' then 'Fundamental II - 9º Ano'
        when '15' then 'Ensino Médio - 1º Ano'
        when '16' then 'Ensino Médio - 2º Ano'
        when '17' then 'Ensino Médio - 3º Ano'
        when '18' then 'Ensino Superior'
        when '' then 'Não informado'
        when '0' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_serie_cursando,
    safe_cast(p_sm.indrecbenef as string) as recebe_beneficio,
    case trim(safe_cast(p_sm.indrecbenef as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_recebe_beneficio,  
    safe_cast(p_sm.indqualbenef as string) as qual_beneficio,
    safe_cast(p_sm.indcuratela as string) as possui_curador,
    case safe_cast(p_sm.indcuratela as string)
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_possui_curador,
    safe_cast(p_sm.indtipcuratela as string) as tipo_curador,
    case trim(safe_cast(p_sm.indtipcuratela as string))
        when '1' then 'Família Extensa'
        when '2' then 'Público'
        when '3' then 'Conhecido/Amigo'
        when '4' then 'Conselho de contabilidade'
        when '' then 'Não possui curador'
        when null then 'Não possui curador'
        else 'Não classificado'
    end as descricao_tipo_curador,
    safe_cast(p_sm.nmcurador as string) as nome_curador,
    safe_cast(p_sm.telcurador as string) as telefone_curador,
    safe_cast(p_sm.indpresdefi as string) as possui_deficiencia,
    case trim(safe_cast(p_sm.indpresdefi as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não possui'
        when null then 'Não possui'
        else 'Não classificado'
    end as descricao_possui_deficiencia,
    safe_cast(p_sm.indtpdefi as string) as tipo_deficiencia,
    safe_cast(p_sm.datobito as date) as data_obito_paciente,
    safe_cast(p_sm.indtpobito as string) as tipo_obito_paciente,
    case trim(safe_cast(p_sm.indtpobito as string))
        when '1' then 'Causa externa'
        when '2' then 'Acidente de trabalho'
        when '3' then 'Causa natural'
        when '' then 'Não possui'
        when null then 'Não possui'
        else 'Não classificado'
    end as descricao_tipo_obito_paciente,
    safe_cast(p_sm.indtpcausaex as string) as tipo_causa_externa_obito,
    case trim(safe_cast(p_sm.indtpcausaex as string))
        when '01' then 'Acidente'
        when '02' then 'Suicídio'
        when '03' then 'Homicídio'
        when '04' then 'Violência'
        when '99' then 'Sem informação'
        when '' then 'Não possui'
        when null then 'Não possui'
        else 'Não classificado'
    end as descricao_tipo_causa_externa_obito,
    safe_cast(p_sm.nminstitu as string) as instituicao_ensino,
    safe_cast(p_sm.tpclasse as string) as classe_instituicao_ensino,
    safe_cast(p_sm.statobsv	as string) as status_alta_transferencia,
    safe_cast(p_sm.morafamacoend as string) as endereco_familia_acolhedora,
    safe_cast(p_sm.morainstnome as string) as instituicao_psico_social,
    safe_cast(p_sm.morainstdtini as date) as data_entrada_instituicao,
    safe_cast(p_sm.morainstdtsai as date) as data_saida_instituicao,
    safe_cast(p_sm.indpacinst as string) as paciente_institucionalizado,
    case trim(safe_cast(p_sm.indpacinst as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_paciente_institucionalizado,
    safe_cast(p_sm.sequsinst as int64) as id_instituicao,
    safe_cast(p_sm.equipesf as string) as equipe_saude_familia,
    safe_cast(p_sm.codcid10 as string) as codigo_cid10_primario,
    safe_cast(p_sm.codciap2 as string) as codigo_ciap2_primaria,
    safe_cast(p_sm.datcodcid10 as date) as data_inicio_cid10_primario,
    safe_cast(p_sm.datcodciap2 as date) as data_inicio_ciap2_primario,
    safe_cast(p_sm.indmediador as string) as mediador_ensino,
    case trim(safe_cast(p_sm.indmediador as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_mediador_ensino,
    safe_cast(p_sm.indrecur as string) as sala_recurso,
    case trim(safe_cast(p_sm.indrecur as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_sala_recurso, 
    safe_cast(p_sm.codorigem as int64) as id_origem_paciente,
    safe_cast(p_sm.datorigempac as date) as data_origem_paciente,
    safe_cast(p_sm.dschistpront as string) as historico_prontuario,
    safe_cast(p_sm.indtratjudicial as string) as tratamento_judicial,
    case trim(safe_cast(p_sm.indtratjudicial as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tratamento_judicial,
    safe_cast(p_sm.dsctrabasempres as string) as empresa_trabalho_assistido,
    safe_cast(p_sm.dsctrabasperiod as string) as data_inicio_trabalho_assistido,
    safe_cast(p_sm.seqequipedeamb as int64) as id_equipe_deambulatorio,
    safe_cast(p_sm.codcid10sec as string) as codigo_cid10_secundario,
    safe_cast(p_sm.datcodcid10sec as date) as data_cid10_secundario,
    safe_cast(p_sm.datinffreqesc as date) as data_frequencia_escola,
    safe_cast(p_sm.dscnaofreqesc as string) as descricao_nao_frequencia_escola,
    safe_cast(p_sm.dscinfadic as string) as contatos_adicionais,
    safe_cast(p_sm.dsctipovinc as string) as vinculo_trabalho_paciente,
    safe_cast(p_sm.dscocupform as string) as ocupacao_formal,
    safe_cast(p_sm.nomempresform as string) as empresa_vinculo_formal,
    safe_cast(p_sm.dscsperiodiniform as string) as inicio_vinculo_formal,
    safe_cast(p_sm.indpcdform as string) as ocupacao_pessoa_deficiente,
    case trim(safe_cast(p_sm.indpcdform as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_ocupacao_pessoa_deficiente, 
    safe_cast(p_sm.dsclocalinform as string) as local_ocupacao_informal,
    safe_cast(p_sm.dscsperiodiniinform as string) as inicio_trabalho_informal,
    safe_cast(p_sm.dscocupassist as string) as local_trabalho_assistido,
    safe_cast(p_sm.dscenderassist as string) as endereco_trabalho_assistido,
    safe_cast(p_sm.dsctelassist as string) as telefone_trabalho_assistido,
    safe_cast(p_sm.indpcdassist as string) as trabalho_pessoa_deficiente,
    case trim(safe_cast(p_sm.indpcdassist as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_trabalho_pessoa_deficiente,
    safe_cast(p_sm.indreinsassist as string) as reinsercao_assistido,
    case trim(safe_cast(p_sm.indreinsassist as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_reinsercao_assistido,
    cp._airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_cidadao_pac') }} cp
inner join {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_pacientes_sm') }} p_sm on p_sm.seqpacsm = cp.seqpac