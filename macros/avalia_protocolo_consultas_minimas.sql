-- Create Macro
{% macro avalia_protocolo_consultas_minimas(dias_de_nascimento, intervalo_temporal_min, intervalo_temporal_max, quant_minima) %}
    CASE 
        WHEN {{dias_de_nascimento}} < {{ intervalo_temporal_min }}
        THEN 'Não Aplicável'
        WHEN {{dias_de_nascimento}} >= {{ intervalo_temporal_min }} and {{dias_de_nascimento}} < {{ intervalo_temporal_max }}
        THEN 'Atenção'
        ELSE 
            CASE 
                WHEN count(
                    case
                        when 
                            distancia_dias >= {{ intervalo_temporal_min }} and 
                            distancia_dias <= {{ intervalo_temporal_max }} 
                        then 1
                        else null 
                    end
                    ) >= {{ quant_minima }}
                THEN 'Aprovado por Mérito'
            ELSE 'Reprovado'
        END
    END
{% endmacro %}