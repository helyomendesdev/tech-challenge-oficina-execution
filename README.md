# Execution Service

Serviço Python que consome comandos da Saga, persiste estado e Outbox no
DynamoDB e publica respostas pelo SQS. O domínio não depende de AWS; a
composição em `execution_service/app.py` usa os adapters de produção.

## Contrato verificado

Fontes consultadas na tag v1.1.0 de
[`tech-challenge-oficina-saga`](https://github.com/helyomendesdev/tech-challenge-oficina-saga/tree/v1.1.0):

- `contrato/asyncapi.yaml`
- `contrato/schemas/envelope.schema.json`
- schemas dos comandos e respostas de execução
- `contrato/saga-estados.yaml`
- `docs/adr-004-pecas-consumidas.md`
- `tracing/propagacao-sqs.md`

O Execution recebe `IniciarExecucao` e `CancelarExecucao` e responde com
`ExecucaoIniciada`, `ExecucaoFinalizada`, `ExecucaoFalhou` ou
`ExecucaoCancelada`. O início e a finalização de cada serviço são operações
internas, não mensagens da Saga.
`itemPecaOSId` é opcional em `IniciarExecucao.pecas[]`; quando disponível, o
Execution rastreia o consumo por reserva e inclui `pecasConsumidas[]` em
`ExecucaoFinalizada` conforme o formato v1.1.0.

O envelope e os payloads são validados sem adicionar campos ao contrato.
`traceparent` e `tracestate` são propagados em SQS `MessageAttributes`, nunca
no corpo. O `requestId`, `sagaId` e `causationId` permanecem no envelope.

Para SQS FIFO, `MessageGroupId=sagaId` e
`MessageDeduplicationId=messageId`.

## Processamento, retry e idempotência

O consumidor usa long polling e só exclui a mensagem depois que:

1. o estado global, o ciclo dos serviços, a Inbox e a resposta de Outbox forem
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

- `ExecutionWorkUseCase` oferece entrada de aplicação interna para iniciar e
  finalizar serviços, mas não existe neste repositório um worker que determine
  quando o trabalho mecânico real termina. A aplicação que executa esse trabalho
  ainda precisa invocar essa porta.
- O Execution persiste a compensação local e emite `ExecucaoCancelada`. A
  liberação física das reservas continua pertencendo ao serviço dono do estoque
  (OS Service); este repositório não acessa o banco de outro serviço.
- Não há worker que detecte automaticamente falhas mecânicas. A aplicação pode
  registrar uma falha antes da criação da execução por
  `ExecuteCommandUseCase.fail_execution_start` ou uma falha interna por
  `ExecutionWorkUseCase.fail_service`; ambas emitem `ExecucaoFalhou` conforme
  o contrato v1.1.
- A fila compartilhada de respostas é fornecida por ARN e não é criada pelo
  Terraform deste repositório.
- O relay é pelo menos uma vez: se o envio ao SQS ocorrer e a confirmação da
  Outbox falhar, a mesma resposta pode ser reenviada. A Outbox mantém o mesmo
  `messageId`; a deduplicação FIFO do SQS é limitada no tempo e não substitui a
  idempotência do consumidor.

## Ciclo interno dos serviços

`ExecutionWorkUseCase` expõe `start_service`, `record_consumption`,
`finish_service` e `fail_service` à camada de aplicação; não foi criada rota
HTTP nem mensagem por serviço. A aplicação inicia o serviço e registra a data,
registra em operação idempotente as peças e quantidades efetivamente
consumidas (validando contra as reservas), e então conclui ou falha o serviço.
Só a conclusão do último serviço cria `ExecucaoFinalizada`. `fail_service`
registra atomicamente a execução como `FALHOU` e emite `ExecucaoFalhou`; a
compensação posterior por `CancelarExecucao` preserva o estado `FALHOU` do
serviço. `pecasConsumidas` é agregado por `itemPecaOSId` e a transição final é
persistida junto com o estado, recibo idempotente e Outbox.

O agregado dos serviços é criado atomicamente com `IniciarExecucao`,
`ExecucaoIniciada`, Inbox e Outbox. DynamoDB usa transações e controle otimista
de versão para serializar atualizações concorrentes; a aplicação repete a
finalização quando outro serviço vence primeiro a disputa.

`CancelarExecucao` registra o motivo, cancela serviços não concluídos e persiste
`ExecucaoCancelada` na mesma transação que a Inbox/Outbox. Também aceita uma
execução no estado `FALHOU`; o comando é uma solicitação de cancelamento, não um
gatilho para gerar `ExecucaoFalhou`, e preserva o serviço que já falhou. O
contrato de resposta contém somente `execucaoId`;
a liberação das reservas é responsabilidade do OS Service, proprietário dessa
compensação, e não é simulada por acesso ao banco de outro serviço.

O `ExecutionWorkUseCase` é a entrada interna que um futuro worker de trabalho
mecânico deve chamar. Hoje não há nesse repositório um worker que determine o
progresso físico dos serviços; testes integrados acionam a entrada de aplicação
diretamente, sem alegar essa integração inexistente. A referência consultada no
[Billing Service](https://github.com/helyomendesdev/tech-challenge-oficina-billing)
usa uma Unit of Work transacional para estado, Inbox e Outbox e um relay de
eventos pendentes; o Execution mantém essas gravações atômicas em
`TransactWriteItems` e usa seu relay FIFO recuperável.

### CLI operacional para o driver BDD

`execution_service/cli.py` oferece comandos de processo único para o driver BDD
invocar o mesmo `ExecutionWorkUseCase` usado pela aplicação. A composição cria
`DynamoDBExecutionRepository` e precisa de `EXECUTION_DYNAMODB_TABLE` e
`AWS_REGION` (padrão `us-east-1`); boto3 usa a cadeia padrão de credenciais ou a
role do container. Não passe credenciais como argumentos.

No container já em execução, use `docker exec <container>`:

```sh
docker exec execution-service python -m execution_service.cli start-service \
  --saga-id 11111111-1111-4111-8111-111111111111 \
  --service-id 101

docker exec execution-service python -m execution_service.cli record-consumption \
  --saga-id 11111111-1111-4111-8111-111111111111 \
  --service-id 101 \
  --part 501:10:2 \
  --part 502:11:1

docker exec execution-service python -m execution_service.cli finish-service \
  --saga-id 11111111-1111-4111-8111-111111111111 \
  --service-id 101

docker exec execution-service python -m execution_service.cli fail-service \
  --saga-id 11111111-1111-4111-8111-111111111111 \
  --service-id 101 \
  --error-code SERVICO_INDISPONIVEL \
  --error-message "Serviço indisponível."
```

O `saga-id` é o `sagaId` da execução; `service-id` é o `servicoId` da
solicitação `IniciarExecucao`. Cada `--part` segue
`ITEM_PECA_OS_ID:PECA_ID:QUANTIDADE` e pode ser repetido. Use os identificadores
de reserva (`itemPecaOSId` e `pecaId`) da execução. `--message-id` pode ser
fornecido em qualquer comando para que uma repetição use a mesma chave
idempotente; sem ele, a CLI gera um UUID novo. `--request-id` é opcional.
`--started-at` e `--completed-at` também são opcionais, aceitam data/hora ISO
8601 com fuso, e quando omitidos usam o horário atual.
Para repetir início/finalização com o mesmo `--message-id`, passe também o
mesmo timestamp: se omitido, o horário atual é gerado novamente em cada
invocação. No consumo, repita os mesmos `--part` com a mesma chave.

A sintaxe completa é:

```text
python -m execution_service.cli start-service --saga-id UUID --service-id ID
    [--started-at ISO_DATETIME] [--request-id ID] [--message-id UUID]
python -m execution_service.cli record-consumption --saga-id UUID --service-id ID
    --part ITEM_PECA_OS_ID:PECA_ID:QUANTIDADE
    [--part ITEM_PECA_OS_ID:PECA_ID:QUANTIDADE ...]
    [--request-id ID] [--message-id UUID]
python -m execution_service.cli finish-service --saga-id UUID --service-id ID
    [--completed-at ISO_DATETIME] [--request-id ID] [--message-id UUID]
python -m execution_service.cli fail-service --saga-id UUID --service-id ID
    --error-code CODE --error-message MESSAGE [--request-id ID] [--message-id UUID]
```

Cada comando válido escreve uma única resposta JSON. Sucesso vai para stdout,
retorna código `0` e informa `status`, `operation`, `sagaId`, `serviceId`,
`serviceStatus`, `messageId`, `duplicate`, `executionCompleted` e
`outboxMessageType`. Erros operacionais vão para stderr, retornam código `1` e
informam `status: "error"` e `error` (`code`, `type` e `message`). Argumentos
inválidos vão para stderr no mesmo formato, com código `2` e
`error.code: "invalid_arguments"`. Exemplo de sucesso:

```json
{"status":"success","operation":"start-service","sagaId":"11111111-1111-4111-8111-111111111111","serviceId":101,"serviceStatus":"EM_EXECUCAO","messageId":"22222222-2222-4222-8222-222222222222","duplicate":false,"executionCompleted":false,"outboxMessageType":null}
```

Não há comando para concluir a execução global: o caso de uso conclui a execução
automaticamente ao finalizar o último serviço, persiste `ExecucaoFinalizada` na
Outbox e a resposta desse comando contém `"executionCompleted": true` e
`"outboxMessageType": "ExecucaoFinalizada"`. `fail-service` registra a falha de
um serviço em execução, altera o estado global para `FALHOU` e persiste
`ExecucaoFalhou` na Outbox na mesma transação. O payload implementado contém
`orcamentoId`, `execucaoId` e `erro` (`codigo`, `mensagem`), como no contrato
v1.1; `execucaoId` é nulo se `IniciarExecucao` falhar antes da criação da
execução. A resposta preserva o `requestId` original e usa o `messageId` de
`IniciarExecucao` como `causationId`. Nesse caso, `executionCompleted`
permanece `false`, enquanto `outboxMessageType` informa `ExecucaoFalhou`. A
CLI não cria uma mensagem nova para a Saga. A compatibilidade foi conferida
contra o [schema v1.1.0 de `ExecucaoFalhou`](https://github.com/helyomendesdev/tech-challenge-oficina-saga/blob/v1.1.0/contrato/schemas/respostas/ExecucaoFalhou.schema.json),
seu [exemplo oficial](https://github.com/helyomendesdev/tech-challenge-oficina-saga/blob/v1.1.0/contrato/exemplos/ExecucaoFalhou.json)
e a [máquina de estados v1.1.0](https://github.com/helyomendesdev/tech-challenge-oficina-saga/blob/v1.1.0/contrato/saga-estados.yaml).

Os exemplos são executados com os IDs reais de uma execução ativa: serviço deve
pertencer à execução e cada peça/quantidade deve estar reservada e ainda
disponível. A CLI apenas encaminha a operação ao caso de uso, que mantém as
validações, idempotência e controle otimista; o repositório salva estado e
Outbox transacionalmente. A imagem já copia todo o pacote
`execution_service/`, portanto `Dockerfile` não precisa de alteração. O código
do Billing não está presente neste checkout; seu padrão não foi duplicado.

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
python -m pip install --require-hashes -r requirements.txt
pytest
```

O processo de produção requer as variáveis em `.env.example` no ambiente. O
arquivo `.env` não é carregado automaticamente. Boto3 usa a cadeia padrão de
credenciais AWS: credenciais temporárias em `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` e `AWS_SESSION_TOKEN` têm suporte, assim como profiles
locais e roles de workload. Não configure segredos no código nem os versione.

```powershell
$env:AWS_REGION = "REGIAO_CONFIGURADA"
$env:EXECUTION_COMMANDS_QUEUE_URL = "URL_DA_FILA_DE_COMANDOS"
$env:SAGA_RESPONSES_QUEUE_URL = "URL_DA_FILA_DE_RESPOSTAS"
$env:EXECUTION_DYNAMODB_TABLE = "TABELA_CONFIGURADA"
python -m execution_service.app
```

### AWS Academy Learner Lab

1. Inicie o Learner Lab e, em **AWS Details**, copie as credenciais temporárias
   da opção **AWS CLI** para variáveis de ambiente na sessão local do terminal:
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` e `AWS_SESSION_TOKEN`. Não as
   grave em `.env`, `.tfvars`, no Dockerfile ou no repositório. Elas expiram;
   obtenha um novo conjunto no Learner Lab e atualize as variáveis da sessão
   antes de continuar.
   No PowerShell, os prompts abaixo leem cada valor sem exibi-lo:

   ```powershell
   $env:AWS_ACCESS_KEY_ID = [Net.NetworkCredential]::new("", (Read-Host -Prompt "AWS_ACCESS_KEY_ID" -AsSecureString)).Password
   $env:AWS_SECRET_ACCESS_KEY = [Net.NetworkCredential]::new("", (Read-Host -Prompt "AWS_SECRET_ACCESS_KEY" -AsSecureString)).Password
   $env:AWS_SESSION_TOKEN = [Net.NetworkCredential]::new("", (Read-Host -Prompt "AWS_SESSION_TOKEN" -AsSecureString)).Password
   $env:AWS_REGION = "REGIAO_CONFIGURADA"
   ```
2. Configure `AWS_REGION`, os URLs das filas e o nome da tabela como no exemplo
   acima. Para conferir a identidade e validade sem expor chaves, execute
   `aws sts get-caller-identity`.
3. Use o Terraform com backend local e desative a criação da policy IAM:
   `terraform -chdir=infra init -backend=false`, depois configure as variáveis
   `TF_VAR_aws_region`, `TF_VAR_execution_dynamodb_table_name` e
   `TF_VAR_saga_responses_queue_arn`, e execute
   `terraform -chdir=infra plan -var="create_runtime_policy=false"`.
   Aplique apenas após revisar o plano e confirmar permissões/custos da conta.
   O backend S3 requer bucket existente e acesso S3; para o Learner Lab,
   backend local evita essa dependência.
4. No terminal com credenciais válidas e as variáveis de runtime configuradas,
   execute `python -m execution_service.app`. Para Docker, veja o exemplo
   abaixo, que passa também as três variáveis temporárias ao container; o SDK
   mantém a mesma cadeia padrão.

O Terraform cria filas SQS e tabela/índice DynamoDB, mas pode não ter permissão
para provisionar todos esses recursos no Learner Lab. Em runtime, o principal
fornecido pela conta precisa autorizar `sqs:ReceiveMessage`,
`sqs:DeleteMessage`, `sqs:SendMessage`, `dynamodb:GetItem`,
`dynamodb:TransactWriteItems`, `dynamodb:UpdateItem` e `dynamodb:Query` no
índice da Outbox. Se o Learner Lab negar alguma ação, use recursos já
provisionados e permitidos ou execute em uma conta com as permissões necessárias;
não há bypass no serviço.

## Docker

```powershell
docker build -t execution-service .
docker run --rm `
  -e AWS_REGION `
  -e AWS_ACCESS_KEY_ID `
  -e AWS_SECRET_ACCESS_KEY `
  -e AWS_SESSION_TOKEN `
  -e EXECUTION_COMMANDS_QUEUE_URL `
  -e SAGA_RESPONSES_QUEUE_URL `
  -e EXECUTION_DYNAMODB_TABLE `
  execution-service
```

## Terraform, CI/CD e deploy

O módulo em `infra/` contém as filas de comando e DLQ conforme o contrato, a
tabela DynamoDB para estado/inbox/outbox e seu índice de pendências, e uma
policy IAM de runtime opcional com permissões limitadas a esses recursos e à
fila de respostas fornecida por ARN. Para o Learner Lab, desative a criação
com `create_runtime_policy=false`; para a conta convencional, o padrão cria a
policy para anexar à identidade de workload já existente.
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
provisionamento dos recursos compartilhados e da integração do worker de
execução mecânica com `ExecutionWorkUseCase`.

O deploy por GitHub Actions é voltado a uma conta convencional: exige uma role
com trust OIDC para GitHub e permissões de ECR/EKS, além da identidade do
workload no cluster. O fluxo local com credenciais temporárias do Learner Lab
não provisiona nem substitui essa configuração.
