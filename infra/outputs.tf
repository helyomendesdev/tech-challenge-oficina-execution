output "execution_commands_queue_url" {
  value = aws_sqs_queue.execution_commands.url
}

output "execution_commands_dlq_url" {
  value = aws_sqs_queue.execution_commands_dlq.url
}

output "execution_table_name" {
  value = aws_dynamodb_table.execution_store.name
}

output "execution_table_arn" {
  value = aws_dynamodb_table.execution_store.arn
}

output "execution_runtime_policy_arn" {
  value = aws_iam_policy.execution_runtime.arn
}
