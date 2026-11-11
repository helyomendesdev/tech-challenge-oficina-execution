# Terraform resources

This module provisions the command FIFO queue and its DLQ, the service-owned
DynamoDB table and pending-outbox index, plus an optional least-privilege
runtime IAM policy. The Saga response queue is an existing shared resource and
must be passed by ARN; it is not created here.

The queue names, FIFO settings, 30-second visibility timeout, and redrive count
of five follow `tech-challenge-oficina-saga` tag `v1.1.0`. The DynamoDB table
uses `pk`/`sk` to atomically store execution and service lifecycle state, inbox
idempotency keys, service-command receipts, and response outbox messages.

## AWS Academy Learner Lab

Start the Learner Lab, then copy the temporary AWS CLI credentials from **AWS
Details** into the environment of the current terminal:
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`. Set
`AWS_REGION` to an enabled region. Boto3 and the Terraform AWS provider use
their normal credential chains; do not put credentials in `.tfvars`, `.env`,
Terraform files, or this repository. Renew the variables with a fresh set
from the Learner Lab when they expire.

Use local Terraform state to avoid requiring an S3 backend bucket:

```powershell
$env:TF_VAR_aws_region = $env:AWS_REGION
$env:TF_VAR_execution_dynamodb_table_name = "execution-service"
$env:TF_VAR_saga_responses_queue_arn = "ARN_DA_FILA_FIFO_COMPARTILHADA"
terraform -chdir=infra init -backend=false
terraform -chdir=infra plan -var="create_runtime_policy=false"
```

Review the plan before applying. Creating queues and a DynamoDB table/index
still requires the Learner Lab principal to have the corresponding SQS and
DynamoDB permissions. The runtime policy is an `aws_iam_policy` resource;
Learner Lab accounts may prohibit IAM changes. Set
`create_runtime_policy=false` to omit that resource; the output
`execution_runtime_policy_arn` will be `null`. This does not grant application
permissions: the Academy-provided principal must already be authorized for the
SQS and DynamoDB operations listed below. An `AccessDenied` means the lab role
or account policy does not permit that action; use resources provisioned by an
authorized account or a conventional AWS account rather than attempting to
bypass the restriction.

The module does not create an S3 state bucket. If using the remote backend
instead, the bucket must already exist and the credentials must have S3 access:

```powershell
terraform -chdir=infra init `
  -backend-config="bucket=$env:TF_STATE_BUCKET" `
  -backend-config="key=execution-service/terraform.tfstate" `
  -backend-config="region=$env:AWS_REGION"
```

## Conventional AWS accounts

By default, `create_runtime_policy=true` preserves the least-privilege
`execution_runtime_policy_arn` for attachment to the workload's existing
identity, such as a Kubernetes service-account role. The policy authorizes:

- SQS `ReceiveMessage` and `DeleteMessage` on the command queue, and
  `SendMessage` on the shared Saga response queue.
- DynamoDB `GetItem`, `TransactWriteItems`, and `UpdateItem` on the service
  table, and `Query` on its pending-outbox index.

The module creates neither IAM roles nor trust relationships, EKS, ECR,
Kubernetes resources, nor the shared response queue. The repository's CD
workflow separately relies on GitHub OIDC to assume a deployment role and on
pre-existing ECR/EKS resources; it is not an AWS Academy deployment workflow.

Set `aws_region`, `execution_dynamodb_table_name`, and
`saga_responses_queue_arn` using `TF_VAR_*` environment variables or a local
`.tfvars` file. Do not store credentials in that file. Run
`terraform fmt -check -recursive`, `terraform init`, and `terraform validate`
after configuring provider credentials. This repository does not run
`terraform apply` automatically.
