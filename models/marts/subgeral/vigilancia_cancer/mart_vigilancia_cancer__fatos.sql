-- noqa: disable=LT08

{{
  config(
    enabled=true,
    schema="projeto_vigilancia_cancer",
    alias="fatos",
    unique_key=['sistema_origem', 'id_sistema_origem'],
    partition_by={
      "field": "data_solicitacao",
      "data_type": "date",
      "granularity": "month",
    },
    cluster_by=['sistema_origem', 'id_cnes_unidade_origem', 'id_cnes_unidade_executante', 'paciente_cpf'],
    on_schema_change='sync_all_columns'
  )
}}

with
sisreg as (
    select 
        -- pk
        "SISREG" as sistema_origem,
        "REGULACAO" as sistema_tipo,
        safe_cast(id_solicitacao as int) as id_sistema_origem,

        -- paciente
        paciente_cpf,
        paciente_cns,

        -- unidades
        id_cnes_unidade_solicitante as id_cnes_unidade_origem,
        id_cnes_unidade_executante,

        -- attr
        solicitacao_risco as carater,
        cid_solicitacao as cid,

        -- proced
        sheets.especialidade as procedimento_especialidade,
        sheets.tipo_procedimento as procedimento_tipo,        
        procedimento,

        -- datas
        safe_cast(data_solicitacao as date) as data_solicitacao,
        safe_cast(data_autorizacao as date) as data_autorizacao,
        safe_cast(data_execucao as date) as data_execucao,
        --data_cancelamento,

        -- resultados siscan
        cast(NULL as date) as data_exame_resultado,
        cast(NULL as string) as mama_esquerda_classif_radiologica,
        cast(NULL as string) as mama_direita_classif_radiologica

    from {{ref("mart_sisreg__solicitacoes")}} as fcts
    left join {{ ref("raw_sheets__assistencial_procedimento") }} as sheets
    on safe_cast(fcts.id_procedimento_sisreg as int) = safe_cast(sheets.id_procedimento as int)
    where 1 = 1
        and data_solicitacao >= "2021-01-01"
        and fcts.procedimento in (
            "MAMOGRAFIA BILATERAL",
            "MAMOGRAFIA  DIAGNOSTICA",
            "CONSULTA EM MASTOLOGIA",
            "CONSULTA EM GINECOLOGIA - MASTOLOGIA",
            "CONSULTA EM CIRURGIA PLASTICA - REPARADORA - MAMA",
            "BIÓPSIA DE MAMA - LESÃO PALPÁVEL",
            "BIOPSIA DE MAMA GUIADA POR USG",
            "BIOPSIA DE MAMA POR ESTEREOTAXIA",
            "ULTRASSONOGRAFIA DE MAMAS BILATERAL",
            "ULTRA-SONOGRAFIA DE MAMAS  BILATERAL",
            "ULTRA-SONOGRAFIA  DE MAMAS (BILATERAL) - PEDIATRICA",
            "ULTRA-SONOGRAFIA DOPPLER DE MAMAS",
            "ULTRASSONOGRAFIA MAMARIA BILATERAL PARA ORIENTAR BIOPSIA DE MAMA",
            "RESSONANCIA MAGNETICA DE MAMA (BILATERAL)",
            "RESSONANCIA MAGNETICA DE MAMA ESQUERDA",
            "RESSONANCIA MAGNETICA DE MAMA DIREITA",

            "MAMOGRAFIA BILATERAL - PPI",
            "CONSULTA EM GINECOLOGIA - MASTOLOGIA - PPI",
            "BIÓPSIA DE MAMA - LESÃO PALPÁVEL - PPI",
            "BIOPSIA DE MAMA GUIADA POR USG-PPI",
            "ULTRA-SONOGRAFIA DE MAMAS BILATERAL - PPI",
            "ULTRA-SONOGRAFIA DOPPLER DE MAMAS - PPI"
        )
),

ser_ambulatorial as (
    select
        -- pk
        "SER" as sistema_origem,
        "REGULACAO" as sistema_tipo,
        id_solicitacao as id_sistema_origem,

        -- paciente
        paciente_cns, 
        
        -- unidades
        id_cnes_unidade_origem,
        id_cnes_unidade_executante,

        -- attr
        carater,
        cid,

        -- procedimento
        coalesce(especialidade_solicitado, especialidade_regulado) as procedimento_especialidade,
        coalesce(procedimento_solicitado_tipo, procedimento_regulado_tipo) as procedimento_tipo,
        coalesce(procedimento_regulado, procedimento_solicitado) as procedimento,

        -- datas
        data_solicitacao,
        data_agendamento as data_autorizacao,
        --data_tratamento_inicio,
        --data_tratamento_prevista,
        data_execucao,

        -- resultados siscan
        cast(NULL as date) as data_exame_resultado,
        cast(NULL as string) as mama_esquerda_classif_radiologica,
        cast(NULL as string) as mama_direita_classif_radiologica

    from {{ ref("raw_ser_metabase__ambulatorial") }}
    where 1 = 1
        and data_solicitacao >= "2021-01-01"
        and (
            procedimento_solicitado in (
                "AMBULATÓRIO 1ª VEZ - MASTOLOGIA (ONCOLOGIA)",
                "RESSONÂNCIA MAGNÉTICA DE MAMA",
                "BIÓPSIA DE MAMA GUIADA POR USG",
                "AMBULATÓRIO 1ª VEZ EM CIRURGIA PLÁSTICA REPARADORA - MAMA (ONCOLOGIA)",
                "MAMOGRAFIA BILATERAL",
                "BIÓPSIA DE MAMA POR ULTRASSONOGRAFIA",
                "BIÓPSIA GUIADA POR MAMOGRAFIA",
                "CORE BIOPSIA DE MAMA",
                "RESSONÂNCIA MAGNÉTICA DE MAMA-ONCOLOGIA",
                "ULTRA-SONOGRAFIA DOPPLER DE MAMAS",
                "ULTRASSONOGRAFIA DE MAMA (FEMININA E MASCULINA)",
                "MASTOLOGIA (RETORNO)",
                "CREG BL AMBULATÓRIO 1ª VEZ - MASTOLOGIA",
                "MAMOGRAFIA - BILATERAL",
                "BIOPSIA DE MAMA POR PAAF",
                "AMBULATÓRIO 1ª VEZ - MASTOLOGIA",
                "AMBULATÓRIO 1ª VEZ EM MASTOLOGIA - LESÃO IMPALPÁVEL (ONCOLOGIA)",
                "CONSULTA EM MASTOLOGIA",
                "MAMOGRAFIA DE RASTREIO",
                "ULTRASSONOGRAFIA - MAMAS",
                "BIÓPSIA DE MAMA POR ESTEREOTAXIA / MAMOTOMIA",
                "CONSULTA EM GINECOLOGIA - MASTOLOGIA",
                "BIÓPSIA DE MAMA - LESÃO PALPÁ�VEL",
                "ULTRASSONOGRAFIA DE MAMA COM DOPPLER",
                "CORE BIÓPSIA DE MAMA",
                "PROCEDIMENTOS DIAGNÓSTICOS GUIADOS POR USG (MAMA) (DESATIVADO)",
                "ULTRASSONOGRAFIA DE MAMAS BILATERAL",
                "BIÓPSIA DE MAMA POR ULTRASSONOGRAFIA"
            ) or 
            procedimento_regulado in (
                "AMBULATÓRIO 1ª VEZ - MASTOLOGIA (ONCOLOGIA)",
                "RESSONÂNCIA MAGNÉTICA DE MAMA",
                "BIÓPSIA DE MAMA GUIADA POR USG",
                "AMBULATÓRIO 1ª VEZ EM CIRURGIA PLÁSTICA REPARADORA - MAMA (ONCOLOGIA)",
                "MAMOGRAFIA BILATERAL",
                "BIÓPSIA DE MAMA POR ULTRASSONOGRAFIA",
                "BIÓPSIA GUIADA POR MAMOGRAFIA",
                "CORE BIOPSIA DE MAMA",
                "RESSONÂNCIA MAGNÉTICA DE MAMA-ONCOLOGIA",
                "ULTRA-SONOGRAFIA DOPPLER DE MAMAS",
                "ULTRASSONOGRAFIA DE MAMA (FEMININA E MASCULINA)",
                "MASTOLOGIA (RETORNO)",
                "CREG BL AMBULATÓRIO 1ª VEZ - MASTOLOGIA",
                "MAMOGRAFIA - BILATERAL",
                "BIOPSIA DE MAMA POR PAAF",
                "AMBULATÓRIO 1ª VEZ - MASTOLOGIA",
                "AMBULATÓRIO 1ª VEZ EM MASTOLOGIA - LESÃO IMPALPÁVEL (ONCOLOGIA)",
                "CONSULTA EM MASTOLOGIA",
                "MAMOGRAFIA DE RASTREIO",
                "ULTRASSONOGRAFIA - MAMAS",
                "BIÓPSIA DE MAMA POR ESTEREOTAXIA / MAMOTOMIA",
                "CONSULTA EM GINECOLOGIA - MASTOLOGIA",
                "BIÓPSIA DE MAMA - LESÃO PALPÁ�VEL",
                "ULTRASSONOGRAFIA DE MAMA COM DOPPLER",
                "CORE BIÓPSIA DE MAMA",
                "PROCEDIMENTOS DIAGNÓSTICOS GUIADOS POR USG (MAMA) (DESATIVADO)",
                "ULTRASSONOGRAFIA DE MAMAS BILATERAL",
                "BIÓPSIA DE MAMA POR ULTRASSONOGRAFIA"
            )
        )
),

ser_internacoes as (
    select
        -- pk
        "SER" as sistema_origem,
        "REGULACAO" as sistema_tipo,
        id_solicitacao as id_sistema_origem,

        -- paciente
        paciente_cns,

        -- unidades
        id_cnes_unidade_origem,
        id_cnes_unidade_executante,

        -- attr
        carater,
        cid,

        -- procedimento
        procedimento_especialidade,
        procedimento_tipo,
        procedimento,
        --coalesce(procedimento_leito_regulado_tipo, procedimento_leito_solicitado_tipo) as leito_tipo,

        -- datas
        data_solicitacao,
        data_reserva as data_autorizacao, -- está certo isso? data_reserva = data_autorizacao?
        data_internacao_inicio as data_execucao,
        --data_internacao_termino,
        --data_alta,

        -- resultados siscan
        cast(NULL as date) as data_exame_resultado,
        cast(NULL as string) as mama_esquerda_classif_radiologica,
        cast(NULL as string) as mama_direita_classif_radiologica
    
    from {{ ref("raw_ser_metabase__internacoes") }}
    where 1 = 1
        and data_solicitacao >= "2021-01-01"
        and procedimento in (
            "DRENAGEM DE ABSCESSO DE MAMA",
            "SEGMENTECTOMIA/QUADRANTECTOMIA/SETORECTOMIA DE MAMA EM ONCOLOGIA",
            "MASTOIDECTOMIA RADICAL",
            "MASTOIDECTOMIA SUBTOTAL",
            "RESSECÇAO DE LESAO NAO PALPÁVEL DE MAMA COM MARCAÇAO EM ONCOLOGIA (POR MAMA)",
            "MAMOPLASTIA PÓS-CIRURGIA BARIÁTRICA"
        )
),

-- repensar toda modelagem e decisoes deste cte do siscan
siscan as ( 
    select
        -- pk
        "SISCAN" as sistema_origem,
        "EXAME" as sistema_tipo,
        safe_cast(protocolo_id as int) as id_sistema_origem, 

        -- paciente
        paciente_cns,

        -- unidades
        unidade_solicitante_id_cnes as id_cnes_unidade_origem,
        unidade_prestadora_id_cnes as id_cnes_unidade_executante,

        -- attr,
        mamografia_rastreamento_tipo as carater,
        cast(NULL as string) as cid,

        -- procedimento
        cast(NULL as string) as procedimento_especialidade,
        "RESULTADO DE EXAME" as procedimento_tipo,
        case 
            when mamografia_tipo = "Rastreamento" then "RESULTADO MAMOGRAFIA DE RASTREIO"
            when mamografia_tipo = "Diagnóstico" then "MAMOGRAFIA DIAGNOSTICA"
            else null
        end as procedimento,

        -- datas
        data_solicitacao,
        cast(NULL as date) as data_autorizacao,
        data_realizacao as data_execucao,

        -- resultados siscan
        data_liberacao_resultado as data_exame_resultado,
        mama_esquerda_classif_radiologica,
        mama_direita_classif_radiologica

    from {{ ref("raw_siscan_web__laudos") }}
    where 1 = 1
        and data_solicitacao >= "2025-01-01"
),

fatos as (
    select 
        sistema_origem,
        sistema_tipo,
        id_sistema_origem,
        paciente_cns,
        id_cnes_unidade_origem,
        id_cnes_unidade_executante,
        carater,
        cid,
        procedimento_especialidade,
        procedimento_tipo,
        procedimento,
        data_solicitacao,
        data_autorizacao,
        data_execucao,
        data_exame_resultado,
        mama_esquerda_classif_radiologica,
        mama_direita_classif_radiologica
    from sisreg
    
    union all 

    select 
        sistema_origem,
        sistema_tipo,
        id_sistema_origem,
        paciente_cns,
        id_cnes_unidade_origem,
        id_cnes_unidade_executante,
        carater,
        cid,
        procedimento_especialidade,
        procedimento_tipo,
        procedimento,
        data_solicitacao,
        data_autorizacao,
        data_execucao,
        data_exame_resultado,
        mama_esquerda_classif_radiologica,
        mama_direita_classif_radiologica
    from ser_ambulatorial

    union all

    select 
        sistema_origem,
        sistema_tipo,
        id_sistema_origem,
        paciente_cns,
        id_cnes_unidade_origem,
        id_cnes_unidade_executante,
        carater,
        cid,
        procedimento_especialidade,
        procedimento_tipo,
        procedimento,
        data_solicitacao,
        data_autorizacao,
        data_execucao,
        data_exame_resultado,
        mama_esquerda_classif_radiologica,
        mama_direita_classif_radiologica
    from ser_internacoes

    union all

    select 
        sistema_origem,
        sistema_tipo,
        id_sistema_origem,
        paciente_cns,
        id_cnes_unidade_origem,
        id_cnes_unidade_executante,
        carater,
        cid,
        procedimento_especialidade,
        procedimento_tipo,
        procedimento,
        data_solicitacao,
        data_autorizacao,
        data_execucao,
        data_exame_resultado,
        mama_esquerda_classif_radiologica,
        mama_direita_classif_radiologica
    from siscan       
),

limpa_fatos as (
    select
        sistema_origem,
        sistema_tipo,
        id_sistema_origem,
        paciente_cns,
        id_cnes_unidade_origem,
        id_cnes_unidade_executante,
        upper(carater) as carater,
        left(cid, 3) as cid,
        procedimento_especialidade,
        procedimento_tipo,
        {{ clean_proced_name("procedimento") }} as procedimento,
        data_solicitacao,
        data_autorizacao,
        data_execucao,
        date_diff(data_execucao, data_solicitacao, day) as tempo_espera,
        data_exame_resultado,
        mama_esquerda_classif_radiologica,
        mama_direita_classif_radiologica
    from fatos
),

enriquece_cpf as (
    select 
        safe_cast(cns_cpf.cpf as int) as paciente_cpf,
        limpa_fatos.*
        
    from limpa_fatos 
    left join {{ref("int_dim_paciente__relacao_cns_cpf")}} as cns_cpf
    on safe_cast(limpa_fatos.paciente_cns as int) = safe_cast(cns_cpf.cns as int)
),

enriquece_cid as (
    select 
        ec.*,
        dim_cid.cid_descricao,
        dim_cid.cid_capitulo_descricao 

    from enriquece_cpf ec
    left join (
        select distinct
            categoria.id as cid,
            categoria.descricao as cid_descricao,
            capitulo.descricao as cid_capitulo_descricao
        from {{ref("dim_condicao_cid10")}}
    ) as dim_cid
    using (cid)
),

enriquece_estabelecimento as (
    select
        enriquece_cid.*,
        estabs_origem.nome_fantasia as estabelecimento_origem_nome,
        estabs_origem.esfera as estabelecimento_origem_esfera,
        estabs_origem.id_ap as estabelecimento_origem_ap,
        estabs_origem.endereco_bairro as estabelecimento_origem_bairro,
        estabs_origem.tipo_unidade_alternativo as estabelecimento_origem_tipo,
        estabs_origem.tipo_unidade_agrupado as estabelecimento_origem_tipo_agrupado,
        estabs_origem.estabelecimento_sms_indicador as estabelecimento_origem_sms_indicador,

        estabs_exec.nome_fantasia as estabelecimento_executante_nome,
        estabs_exec.esfera as estabelecimento_executante_esfera,
        estabs_exec.id_ap as estabelecimento_executante_ap,
        estabs_exec.endereco_bairro as estabelecimento_executante_bairro,
        estabs_exec.tipo_unidade_alternativo as estabelecimento_executante_tipo,
        estabs_exec.tipo_unidade_agrupado as estabelecimento_executante_tipo_agrupado,
        estabs_exec.estabelecimento_sms_indicador as estabelecimento_sms_indicador

    from enriquece_cid

    left join {{ref("dim_estabelecimento_sus_rio_historico")}} as estabs_origem
    on safe_cast(id_cnes_unidade_origem as int) = safe_cast(estabs_origem.id_cnes as int)

    left join {{ref("dim_estabelecimento_sus_rio_historico")}} as estabs_exec
    on safe_cast(id_cnes_unidade_origem as int) = safe_cast(estabs_exec.id_cnes as int)

    where 1 = 1
        and estabs_origem.ano_competencia = 2025 and estabs_origem.mes_competencia = 6
        and estabs_exec.ano_competencia = 2025 and estabs_exec.mes_competencia = 6
),

final as (
    select
        -- pk
        sistema_origem,
        id_sistema_origem,

        -- id paciente
        paciente_cpf,
        paciente_cns,

        -- tipo sistema fonte
        sistema_tipo,

        -- qualificacao do procedimento
        procedimento_especialidade,
        procedimento_tipo,
        procedimento,        
        carater,
        cid,
        cid_descricao,
        cid_capitulo_descricao,

        -- datas
        data_solicitacao,
        data_autorizacao,
        data_execucao,
        tempo_espera,

        -- resultados siscan
        data_exame_resultado,
        mama_esquerda_classif_radiologica,
        mama_direita_classif_radiologica,

        -- unidade solicitante
        id_cnes_unidade_origem,
        estabelecimento_origem_nome,
        estabelecimento_origem_esfera,
        estabelecimento_origem_ap,
        estabelecimento_origem_bairro,
        estabelecimento_origem_tipo,
        estabelecimento_origem_tipo_agrupado,
        estabelecimento_origem_sms_indicador,

        -- unidade executante
        id_cnes_unidade_executante,
        estabelecimento_executante_nome,
        estabelecimento_executante_esfera,	
        estabelecimento_executante_ap,
        estabelecimento_executante_bairro,
        estabelecimento_executante_tipo,
        estabelecimento_executante_tipo_agrupado,
        estabelecimento_sms_indicador
    from enriquece_estabelecimento
)

select * from final
