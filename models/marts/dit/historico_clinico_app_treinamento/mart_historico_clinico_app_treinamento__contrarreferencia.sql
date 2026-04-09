{{
    config(
        alias="contrarreferencia",
        schema="app_historico_clinico_treinamento",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

select
    '0123456789abcdeffedcba9876543210' as id_hci,
    'Inst de Medicina Veterinária Jorge Vaitsman' as estabelecimento,

    'Ingrid Dahl Christensen' as profissional_nome,
    'Especialista Meridional' as profissional_cargo,

    '2026000406771' as documento_numero,
    cast('2026-04-01T19:35:12' as DATETIME) as documento_datahora,
    'gs://sarah_documentos/mock/mock-sarah.pdf' as documento_uri,

    'Aguardo chegada de bispo para exorcismo' as conduta,
    cast(null as string) as seguimento,

    trim('''
[HDA]
#80 anos, masculino
#Investigação de DPOC; Ex tabagista de baixa carga + fumo passivo
#Já operou sela túrcica após tomografia computacional apontar problemas
#Relata nunca ter tido febre, meço temperatura corporal múltiplas vezes em 21C
#Suspeito vampirismo; idade e aparência não conferem com suposta data de nascimento
Relata ter caído de escorrega em pátio de construção.
Questionado sobre a existência e acessibilidade de playground em meio a obra corrente, não soube explicar.
Instruo sobre lei da sobrevivência do mais forte de Darwin, recomendo descanso e água.

[MEDICAMENTOS EM USO]
-

[HIPÓTESE DIAGNÓSTICA]
acima
    ''') as resumo,

    trim('''
#80 anos, masculino
#Investigação de DPOC; Ex tabagista de baixa carga + fumo passivo
#Já operou sela túrcica após tomografia computacional apontar problemas
#Relata nunca ter tido febre, meço temperatura corporal múltiplas vezes em 21C
#Suspeito vampirismo; idade e aparência não conferem com suposta data de nascimento
Relata ter caído de escorrega em pátio de construção.
Questionado sobre a existência e acessibilidade de playground em meio a obra corrente, não soube explicar.
Instruo sobre lei da sobrevivência do mais forte de Darwin, recomendo descanso e água.
    ''') as historia_doenca_atual,
    cast(null as string) as medicamentos_em_uso,
    cast(null as string) as hipotese_diagnostica,

    42298037299 as cpf_particao
