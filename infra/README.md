# Terraform resources

This module provisions the command queue and its DLQ, the service-owned
DynamoDB table, and a least-privilege runtime IAM policy. The Saga response
queue is an existing shared resource and must be passed by ARN; it is not
created here.

The queue names, FIFO settings, 30-second visibility timeout, and redrive count
of five follow `tech-challenge-oficina-saga` branch `contrato/v1`. The DDB
table uses `pk`/`sk` to atomically store execution state, inbox idempotency
keys, and response outbox messages.

The S3 state bucket must already exist and is not created by this module.
Supply backend configuration at initialization:

```powershell
terraform -chdir=infra init `
  -backend-config="bucket=$env:TF_STATE_BUCKET" `
  -backend-config="key=execution-service/terraform.tfstate" `
  -backend-config="region=$env:AWS_REGION"
```

Set `aws_region`, `execution_dynamodb_table_name`,
`saga_responses_queue_arn`, and `execution_runtime_policy_name` using a local
`.tfvars` file or `TF_VAR_*` environment variables. Attach the emitted runtime
policy to the workload's existing identity (for example, its Kubernetes
service-account role). No Kubernetes, ECR, cluster, shared response-queue, or
IAM-role resources are created by this module.

Run `terraform fmt -check` and `terraform validate` after configuring backend
and provider credentials. This repository does not run `terraform apply`.
