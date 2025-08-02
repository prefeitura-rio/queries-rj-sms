{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="beneficios",
        materialized="table",
        tags=["raw", "pcsm"],
        description="Tipos de beneficios sociais que os pacientes recebem."
    )
}} 

select codigo, descricao, current_timestamp() as transformed_at 
  from unnest([
      struct(1 as codigo, 'Aposentadoria' as descricao),
      struct(2, 'Auxílio Doença'),
      struct(3, 'Bolsa BAR [Bolsa de apoio à ressocialização]'),
      struct(4, 'Bolsa de volta para casa [PVC nacional]'),
      struct(5, 'Bolsa Família'),
      struct(6, 'Bolsa-Rio Tipo I'),
      struct(7, 'Bolsa-Rio Tipo II'),
      struct(8, 'Cartão Família Carioca [CFC]'),
      struct(9, 'BPC-Benefício de Prestação Continuada'),
      struct(10, 'Pensão'),
      struct(11, 'Auxílio Emergencial'),
      struct(12, 'Auxílio Brasil'),
      struct(13, 'Assalariado'),
      struct(14, 'Agente Experiente'),
      struct(15, 'Aluguel Social'),
      struct(16, 'Apoio Moradia'),
      struct(17, 'Benefício comprometido em empréstimo'),
      struct(18, 'Benefícios eventuais'),
      struct(19, 'Pensão Alimentícia'),
      struct(20, 'Pensão por morte'),
      struct(21, 'Outro Benefício'),
      struct(22, 'Outro tipo de Pensão'),
      struct(23, 'Não sabe / Não lembra'),
      struct(24, 'Não respondeu'),
      struct(25, 'Bolsa de inclusão produtiva [Seguir em Frente]'),
      struct(26, 'Auxílio moradia temporário'),
      struct(27, 'Idoso em família'),
      struct(28, 'Riocard especial'),
      struct(29, 'Riocard estudante'),
      struct(30, 'Riocard senior'),
      struct(31, 'Acidente de trabalho'),
      struct(32, 'Aposentadoria por invalidez'),
      struct(33, 'Trabalho informal'),
      struct(34, 'Bolsa ONG / OSS'),
      struct(35, 'Militar da Reserva'),
      struct(36, 'Bolsa Estudantil')
  ]) as beneficios