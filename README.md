# Execution Service

Serviço Python que consome comandos de execução da Saga, mantém o estado em
DynamoDB e publica a confirmação pelo SQS. O domínio não depende de AWS; a
composição em `execution_service/app.py` usa os adapters de produção. Os testes
usam repositório e publisher fakes, sem credenciais nem conta AWS.

## Contrato verificado

Fontes consultadas no branch `contrato/v1` de
[`tech-challenge-oficina-saga`](https://github.com/helyomendesdev/tech-challenge-oficina-saga/tree/contrato/v1):

- `contrato/asyncapi.yaml`
- `contrato/schemas/envelope.schema.json`
- schemas dos comandos e respostas de execução
- `contrato/saga-estados.yaml`
- `tracing/propagacao-sqs.md`
- `docs/questoes-abertas.md`
- `scripts/validar_contrato.py`

O Execution reconhece somente estes tipos de comando no envelope:

- `IniciarExecucao`, com `orcamentoId`, `servicos` e `pecas`
- `CancelarExecucao`, com `execucaoId` e `motivo`

`IniciarExecucao` responde com `ExecucaoIniciada`. `CancelarExecucao` é validado,
mas não é executado nem confirmado: permanece sem exclusão na fila e segue a
política de redrive para DLQ. O envelope é validado de forma estrita: UUIDs,
versão 1, campos obrigatórios, ausência de propriedades adicionais, payload e
data RFC 3339. `traceparent` e `tracestate` são lidos e publicados como SQS
`MessageAttributes`, nunca no corpo. O `requestId`, `sagaId` e `causationId`
permanecem no envelope.

O AsyncAPI especifica grupo FIFO igual a `sagaId`, deduplicação igual a
`messageId`, timeout de visibilidade de 30 segundos e DLQ após cinco
recebimentos. O publisher v1 em
`os_service/infrastructure/saga_adapters.py` do OS Service usa
`os_id_group(os_id)`, que produz `os-{osId}`, divergindo do AsyncAPI. O
AsyncAPI é a definição normativa que o Execution implementa, mas não é seguro
alterar unilateralmente o produtor nem presumir qual particionamento o grupo
quer manter. A integração operacional está bloqueada até a equipe alinhar
contrato e publisher.

## Processamento, retry e idempotência

O consumidor usa long polling e só exclui a mensagem depois que:

1. o estado da execução, a chave de idempotência e a resposta de outbox forem
   gravados atomicamente com `TransactWriteItems`;
2. a resposta for enviada à fila FIFO de respostas;
3. o outbox for marcado como publicado.

Em falha na persistência, publicação, confirmação do outbox ou exclusão SQS, a
mensagem de entrada não é confirmada e pode ser reentregue. Um relay em processo
consulta periodicamente o índice persistente de outbox pendentes; por isso,
respostas pendentes continuam recuperáveis após reinício e mesmo que o comando
original acabe na DLQ. Reentregas e o relay reutilizam o mesmo `messageId` de
resposta e payload persistidos. A deduplicação FIFO da resposta é uma proteção
adicional de cinco minutos, não uma transação distribuída: se a publicação
funcionar e a marcação falhar depois dessa janela, a resposta pode ser
publicada novamente, e o consumidor deve respeitar sua chave de idempotência.
Os registros de inbox/outbox não têm TTL, para não expirar chaves de
idempotência automaticamente.

Mensagens inválidas, tipos não consumidos e falhas transitórias não são
excluídos; a fila aplica a política de redrive para DLQ. O adaptador não consome
automaticamente a DLQ.

### Limitações funcionais atuais

- `IniciarExecucao` persiste os serviços e peças informados e confirma que a
  execução foi iniciada. Ainda não há worker ou operação de domínio que realize
  o trabalho mecânico e gere `ExecucaoFinalizada` ou `ExecucaoFalhou`.
- `CancelarExecucao` falha de forma segura, sem alterar a execução, persistir
  inbox/outbox, publicar `ExecucaoCancelada` ou excluir a mensagem SQS. O comando
  será redeliverado e enviado à DLQ pela política configurada. O contrato descreve
  devolução de peças reservadas, mas o serviço não tem worker de interrupção nem
  ownership/adapter de estoque definido; não acessa banco de dados do OS
  Service/Billing. Habilitar o cancelamento exige alinhar e implementar essa
  responsabilidade antes de confirmar o comando.
- No monólito Fase 3, iniciar um item exigia status `PENDENTE`, OS em
  `EXECUCAO`, e consumo de peças dentro das quantidades reservadas. Finalizar
  exigia item `EM_EXECUCAO`, data final não anterior ao início, e só finalizava
  a OS depois de todos os serviços concluídos. Essas regras ainda não foram
  portadas para um worker de execução, pois o contrato v1 não define esse
  ciclo operacional nem uma integração de estoque para este microsserviço.
- O contrato não especifica a propriedade da fila compartilhada de respostas.
  Terraform recebe seu ARN como variável e não cria uma segunda fila.
- O schema v1 define `ExecucaoFalhou` com `orcamentoId`, `execucaoId` (nulo se
  ainda não houver execução) e `erro` (`codigo` e `mensagem`); `ExecucaoFinalizada`
  exige `execucaoId` e `finalizadaEm`. O contrato e o monólito não fornecem um
  comando/evento de conclusão nem um adapter de trabalho mecânico que permita
  produzir essas respostas com segurança. Não há transição de sucesso/falha
  simulada.
- O contrato público está na branch `contrato/v1`; a branch `main` do
  repositório da Saga contém somente o README.
- O serviço propaga `traceparent`/`tracestate` como message attributes, mas não
  exporta spans para New Relic ou outro collector e não implementa métricas.
  Os eventos de log são emitidos em JSON com identificadores de mensagem,
  requisição, Saga e OS quando disponíveis.

## Regras de negócio e extração

O monólito faz `IniciarServico` por item de serviço: exige item `PENDENTE` e OS
em `EXECUCAO`, valida consumo positivo e não superior ao saldo reservado,
registra o consumo e atualiza `quantidade_utilizada`. `FinalizarServico` exige
item `EM_EXECUCAO`, data final não anterior à inicial e OS em `EXECUCAO`;
finaliza a OS apenas quando todos os itens de serviço estão concluídos e as
peças reservadas foram utilizadas. O repositório/Django mantém itens,
consumos, status da OS e transações.

O contrato `IniciarExecucao` só carrega IDs/quantidades de serviços e peças
aprovados; não carrega IDs de itens da OS, estado/reserva atual, datas por
serviço, nem operação de estoque. `CancelarExecucao` exige o ID da execução e
um motivo, e descreve a compensação como devolver peças reservadas, mas o código
de execução do monólito não define um caso de uso de cancelamento independente
nem ownership de estoque para o novo serviço. Por isso, essas operações
originais não foram copiadas para o DynamoDB nem substituídas por sucesso
simulado; concluir o ciclo requer definir o adapter/ownership e o comando de
trabalho mecânico.

## Estrutura de testes e versionamento de dados

`tests/factories.py` reúne fábricas dos envelopes, comandos, registros e eventos;
`tests/conftest.py` expõe fixtures de repositório fake, publisher, processador e
consumidor. `tests/integration/` usa `botocore.stub.Stubber` para exercitar as
chamadas/formatos dos adapters boto3 sem credenciais, endpoint AWS ou
LocalStack. Isso verifica a fronteira SDK, não substitui testes em AWS real.

Não há migrations Django/Alembic: o modelo é versionado pelos recursos
DynamoDB/Terraform, e os itens persistidos incluem `schema_version`. Alterações
de chave ou índice devem ser feitas no Terraform (adicionar novo índice antes
de mudar consumidores). Mudanças incompatíveis de atributos exigem leitura
retrocompatível ou migração de dados versionada e revisada; só remova atributos
antigos após confirmar que não há registros/outbox pendentes no formato
anterior. Não aplique mudanças de tabela manualmente fora do Terraform.

## Execução local e testes

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
pytest
```

O processo de produção requer as variáveis em `.env.example` no ambiente. O
arquivo `.env` não é carregado automaticamente. Boto3 usa a cadeia padrão de
credenciais AWS (por exemplo, role do workload); não configure segredos no
código nem versionamento.

```powershell
$env:AWS_REGION = "REGIAO_CONFIGURADA"
$env:EXECUTION_COMMANDS_QUEUE_URL = "URL_DA_FILA_DE_COMANDOS"
$env:SAGA_RESPONSES_QUEUE_URL = "URL_DA_FILA_DE_RESPOSTAS"
$env:EXECUTION_DYNAMODB_TABLE = "TABELA_CONFIGURADA"
python -m execution_service.app
```

## Docker

```powershell
docker build -t execution-service .
docker run --rm `
  -e AWS_REGION `
  -e EXECUTION_COMMANDS_QUEUE_URL `
  -e SAGA_RESPONSES_QUEUE_URL `
  -e EXECUTION_DYNAMODB_TABLE `
  execution-service
```

## Terraform, CI/CD e deploy

O módulo em `infra/` contém as filas de comando e DLQ conforme o contrato, a
tabela DynamoDB para estado/inbox/outbox e seu índice de pendências, e uma
policy IAM de runtime com permissões limitadas a esses recursos e à fila de
respostas fornecida por ARN.
O bucket S3 do backend precisa ser criado previamente; veja
[`infra/README.md`](infra/README.md) para inicialização e variáveis.

`ci.yml` executa Ruff lint/formatação, pytest com cobertura mínima de 80%, build
Docker e validação Terraform sem backend remoto. `cd.yml` prepara build e
publicação na ECR existente e deploy de workload no EKS existente por OIDC,
condicionados a `EXECUTION_DEPLOY_ENABLED=true`. Deve-se configurar as GitHub
Variables `AWS_REGION`, `ECR_REPOSITORY`, `EKS_CLUSTER_NAME`, `K8S_NAMESPACE`,
`EXECUTION_K8S_SERVICE_ACCOUNT`, `KUBECTL_VERSION`,
`EXECUTION_COMMANDS_QUEUE_URL`, `SAGA_RESPONSES_QUEUE_URL` e
`EXECUTION_DYNAMODB_TABLE`, além do secret `AWS_ROLE_ARN`.

O namespace e o ServiceAccount ligado à identidade AWS devem ser fornecidos
pela infraestrutura compartilhada; este repositório não cria cluster, ECR,
namespace, IAM role ou ServiceAccount. O deploy só renderiza ConfigMap e
Deployment do worker. Não execute `terraform apply` nem habilite CD antes do
alinhamento do `MessageGroupId`, da criação dos recursos compartilhados e da
confirmação da integração de execução mecânica.
