from enum import Enum


class ExecutionStatus(str, Enum):
    EM_EXECUCAO = "EM_EXECUCAO"
    CONCLUIDA = "CONCLUIDA"
    FALHOU = "FALHOU"
    CANCELADA = "CANCELADA"


class SagaMessageType(str, Enum):
    INICIAR_EXECUCAO = "IniciarExecucao"
    CANCELAR_EXECUCAO = "CancelarExecucao"
    EXECUCAO_INICIADA = "ExecucaoIniciada"
    EXECUCAO_FINALIZADA = "ExecucaoFinalizada"
    EXECUCAO_CANCELADA = "ExecucaoCancelada"
    EXECUCAO_FALHOU = "ExecucaoFalhou"
