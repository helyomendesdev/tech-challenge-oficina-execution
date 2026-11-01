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
- O repositório `tech-challenge-oficina-saga` mantém o AsyncAPI e os schemas v1
  em `main`. O AsyncAPI identifica a v1 como proposta para revisão do grupo;
  a aprovação dos mantenedores continua necessária para qualquer extensão.
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

### Bloqueio para o ciclo individual dos serviços

O contrato v1 disponível não permite iniciar/finalizar serviços individualmente
nem fazer a validação de consumo antes de mudar um serviço para
`EM_EXECUCAO`. A fila de comandos define `IniciarExecucao` e `CancelarExecucao`;
as respostas incluem `ExecucaoIniciada`, `ExecucaoFinalizada`,
`ExecucaoFalhou` e `ExecucaoCancelada`. Não existem comandos de início/fim por
serviço, retorno de aceite/rejeição do OS Service ou mensagem de consumo por
serviço. `ExecucaoFinalizada` só contém `execucaoId` e `finalizadaEm`.

No modelo atual do OS Service, `servicoId` é numérico e único por OS pela
restrição `(ordem_servico, servico)`, portanto `(osId, servicoId)` identifica o
serviço enquanto essa invariável for mantida. Para peças, `pecaId` não
identifica necessariamente uma reserva específica: a OS pode ter linhas
`ItemPecaOS` distintas para a mesma peça, e o consumo legado relaciona essas
linhas a um item de serviço. O contrato v1 não inclui o identificador da linha
reservada.

Por isso, o Execution Service não pode implementar com segurança, usando apenas
o contrato atual, a sequência “registrar consumo → aguardar validação/aceite do
OS → marcar serviço `EM_EXECUCAO`”, nem comunicar o consumo por linha da OS. A
tentativa pendente também não pode ser descartada quando houver incerteza sobre
se a validação ou o consumo foram aplicados. Não se deve reutilizar ou ampliar
unilateralmente `IniciarExecucao`, `ExecucaoFinalizada` ou outros tipos v1 para
representar essas operações.

O fluxo permanece bloqueado até OS/Saga aprovarem e publicarem os contratos
necessários para: (1) comandos de início e fim por serviço; (2) solicitação e
resposta de aceite/rejeição do consumo pelo OS Service, incluindo correlação
idempotente; e (3) comunicação do consumo com referência suficiente para a OS
identificar suas reservas. A conclusão no wire deve preservar
`ExecucaoFinalizada`; `EXECUTION_COMPLETED` é somente o evento interno de
domínio, não um segundo tipo de mensagem. Depois desse alinhamento, será
possível implementar a transição pendente de forma retomável, a Outbox com
todas as saídas atômicas e os respectivos handlers/testes, sem acesso ao banco
de outro microsserviço.

#### Núcleo de domínio local

`execution_service/domain/service_execution.py` contém a máquina de estados
local por serviço (`PENDENTE`, `INICIO_PENDENTE`, `EM_EXECUCAO` e `CONCLUIDO`),
validação positiva das quantidades e IDs de reserva (`itemPecaOSId` + `pecaId`),
confirmação explícita do início **e do consumo**, rejeição explícita e
idempotência da tentativa corrente por UUID de mensagem. O chamador só deve
aplicar confirmação/rejeição após validar e persistir a decisão correlacionada
da Saga/OS; a entidade não verifica nem simula essa integração. Timeout ou
falha não têm transição que limpe `INICIO_PENDENTE`.
`ExecutionLifecycle` agrega os serviços por OS e produz o evento de domínio
`EXECUTION_COMPLETED` uma única vez quando todos estão concluídos.

Essas entidades ainda **não estão ligadas** ao consumer, aos Use Cases
operacionais, ao repositório DynamoDB ou à Outbox. Não gravam confirmação ou
rejeição da Saga, não enviam consumo ao OS e não publicam `ExecucaoFinalizada`.
Assim, não há integração simulada nem alegação de fluxo ponta a ponta. O
próximo passo depende da aprovação dos contratos compartilhados de início/fim,
consumo e aceite/rejeição; `itemPecaOSId` deve ser aprovado antes de sair em
mensagens. O domínio mantém somente a tentativa corrente: redeliveries de
mensagens de tentativas antigas, após novas tentativas e rejeições subsequentes,
devem ser deduplicadas pelo histórico durável do repositório antes de chamar a
entidade. A presença do evento em memória não garante publicação única após
reinício; o futuro repositório deve persistir a transição final e a mensagem
`ExecucaoFinalizada` na Outbox na mesma transação, com condição/idempotência
durável. Os testes locais cobrem apenas as regras puras do domínio.

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

## Use Cases locais para serviços

`execution_service/application/service_execution_use_cases.py` contém Use Cases
para iniciar, confirmar, rejeitar e finalizar um serviço. O `ExecuteCommandUseCase`
existente continua responsável pelo comando global v1 `IniciarExecucao`; não foi
duplicado nem alterado. O início individual persiste no agregado como
`INICIO_PENDENTE`, incluindo o consumo solicitado, e só passa a
`EM_EXECUCAO` após a porta `PersistedServiceStartDecisionPort` fornecer uma
decisão durável, explicitamente correlacionada à OS, serviço e tentativa.
Timeouts e resultados desconhecidos não são tratados como rejeição.

Esses Use Cases ainda não estão conectados ao consumer ou à composição de
produção. `ServiceExecutionRepositoryPort` define uma única operação atômica
para persistir estado, recibo idempotente e eventual Outbox; somente o fake em
memória implementa essa porta nesta etapa. O repositório DynamoDB e o relay
existentes persistem o modelo global legado e não leem esses novos registros.
Assim, os testes verificam o contrato atômico da porta e o comportamento dos
Use Cases, mas não alegam persistência durável de produção para o agregado por
serviço.

Quando o último serviço termina, o Use Case mapeia o evento interno
`EXECUTION_COMPLETED` para o formato v1 já existente `ExecucaoFinalizada` e o
entrega à mesma fronteira de commit para gravar a Outbox junto ao estado. O
fluxo ainda não publica essa mensagem em produção: faltam a implementação
DynamoDB dessa porta, sua integração com o relay e um gatilho de finalização
individual aprovado. O contrato v1 não define comandos de serviço, retorno de
aceite/rejeição, nem mensagem de consumo; nenhuma dessas mensagens foi criada.
Em particular, `itemPecaOSId` permanece dado local e não é enviado ao OS
Service até aprovação do contrato compartilhado.

A inicialização do agregado por serviço a partir do `IniciarExecucao` global
também requer uma gravação transacional coordenada com o registro global e sua
resposta `ExecucaoIniciada`; ela não é feita em duas gravações independentes.
Até essa evolução e o alinhamento de contratos serem aprovados, os novos Use
Cases são uma camada local de aplicação, não uma integração ponta a ponta.
`CancelarExecucao` mantém o comportamento seguro já documentado e não foi
alterado.
