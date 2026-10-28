from enum import Enum


class ExecutionStatus(str, Enum):
    EM_EXECUCAO = "EM_EXECUCAO"


class SagaMessageType(str, Enum):
    INICIAR_EXECUCAO = "IniciarExecucao"
    CANCELAR_EXECUCAO = "CancelarExecucao"
    EXECUCAO_INICIADA = "ExecucaoIniciada"
    EXECUCAO_CANCELADA = "ExecucaoCancelada"
