terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {}
}

provider "aws" {
  region = var.aws_region
}

resource "aws_sqs_queue" "execution_commands_dlq" {
  name                        = var.execution_commands_dlq_name
  fifo_queue                  = true
  content_based_deduplication = false
  deduplication_scope         = "messageGroup"
  fifo_throughput_limit       = "perMessageGroupId"
  message_retention_seconds   = 1209600

  sqs_managed_sse_enabled = true
}

resource "aws_sqs_queue" "execution_commands" {
  name                        = var.execution_commands_queue_name
  fifo_queue                  = true
  content_based_deduplication = false
  deduplication_scope         = "messageGroup"
  fifo_throughput_limit       = "perMessageGroupId"
  message_retention_seconds   = 1209600
  visibility_timeout_seconds  = var.visibility_timeout_seconds

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.execution_commands_dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

resource "aws_dynamodb_table" "execution_store" {
  name                        = var.execution_dynamodb_table_name
  billing_mode                = "PAY_PER_REQUEST"
  hash_key                    = "pk"
  range_key                   = "sk"
  deletion_protection_enabled = var.dynamodb_deletion_protection

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "outbox_status"
    type = "S"
  }

  attribute {
    name = "outbox_created_at"
    type = "S"
  }

  global_secondary_index {
    name            = "pending-outbox-index"
    hash_key        = "outbox_status"
    range_key       = "outbox_created_at"
    projection_type = "ALL"
  }

  server_side_encryption {
    enabled = true
  }

  point_in_time_recovery {
    enabled = true
  }
}

data "aws_iam_policy_document" "execution_runtime" {
  statement {
    sid    = "ConsumeExecutionCommands"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
    ]
    resources = [aws_sqs_queue.execution_commands.arn]
  }

  statement {
    sid       = "PublishSagaResponses"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [var.saga_responses_queue_arn]
  }

  statement {
    sid    = "PersistExecutionAndOutbox"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:TransactWriteItems",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.execution_store.arn]
  }

  statement {
    sid       = "QueryPendingOutbox"
    effect    = "Allow"
    actions   = ["dynamodb:Query"]
    resources = ["${aws_dynamodb_table.execution_store.arn}/index/pending-outbox-index"]
  }
}

resource "aws_iam_policy" "execution_runtime" {
  name   = var.execution_runtime_policy_name
  policy = data.aws_iam_policy_document.execution_runtime.json
}
