variable "aws_region" {
  description = "AWS region chosen with the shared infrastructure team."
  type        = string

  validation {
    condition     = length(trimspace(var.aws_region)) > 0
    error_message = "aws_region must not be empty."
  }
}

variable "execution_commands_queue_name" {
  description = "Execution command FIFO queue name from the Saga AsyncAPI contract."
  type        = string
  default     = "oficina-execucao-comandos.fifo"

  validation {
    condition     = endswith(var.execution_commands_queue_name, ".fifo")
    error_message = "The execution commands queue name must end with .fifo."
  }
}

variable "execution_commands_dlq_name" {
  description = "Execution command DLQ name from the Saga AsyncAPI contract."
  type        = string
  default     = "oficina-execucao-comandos-dlq.fifo"

  validation {
    condition     = endswith(var.execution_commands_dlq_name, ".fifo")
    error_message = "The execution commands DLQ name must end with .fifo."
  }
}

variable "execution_dynamodb_table_name" {
  description = "DynamoDB table name assigned to this service by the infrastructure team."
  type        = string

  validation {
    condition = (
      length(var.execution_dynamodb_table_name) >= 3 &&
      length(var.execution_dynamodb_table_name) <= 255
    )
    error_message = "The DynamoDB table name must contain between 3 and 255 characters."
  }
}

variable "saga_responses_queue_arn" {
  description = "ARN of the existing Saga response FIFO queue; this module does not create it."
  type        = string

  validation {
    condition = (
      can(regex("^arn:[^:]+:sqs:[^:]+:[0-9]{12}:.+\\.fifo$", var.saga_responses_queue_arn))
    )
    error_message = "saga_responses_queue_arn must be a valid FIFO SQS queue ARN."
  }
}

variable "execution_runtime_policy_name" {
  description = "IAM policy name for the workload identity used by this service."
  type        = string

  validation {
    condition = (
      length(var.execution_runtime_policy_name) >= 1 &&
      length(var.execution_runtime_policy_name) <= 128
    )
    error_message = "The IAM policy name must contain between 1 and 128 characters."
  }
}

variable "visibility_timeout_seconds" {
  description = "SQS visibility timeout; v1 contract specifies 30 seconds."
  type        = number
  default     = 30

  validation {
    condition = (
      var.visibility_timeout_seconds >= 1 &&
      var.visibility_timeout_seconds <= 43200 &&
      floor(var.visibility_timeout_seconds) == var.visibility_timeout_seconds
    )
    error_message = "visibility_timeout_seconds must be an integer between 1 and 43200."
  }
}

variable "max_receive_count" {
  description = "SQS receive limit before redrive; v1 contract specifies 5."
  type        = number
  default     = 5

  validation {
    condition = (
      var.max_receive_count >= 1 &&
      floor(var.max_receive_count) == var.max_receive_count
    )
    error_message = "max_receive_count must be a positive integer."
  }
}

variable "dynamodb_deletion_protection" {
  description = "Protect the execution and inbox/outbox table against accidental deletion."
  type        = bool
  default     = true
}
