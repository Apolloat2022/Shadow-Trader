###############################################################################
# outputs.tf
# Exported values for use by other Terraform modules or CI/CD pipelines.
###############################################################################

output "bronze_bucket_name" {
  description = "Name of the Bronze S3 bucket."
  value       = aws_s3_bucket.bronze.bucket
}

output "bronze_bucket_arn" {
  description = "ARN of the Bronze S3 bucket."
  value       = aws_s3_bucket.bronze.arn
}

output "silver_bucket_name" {
  description = "Name of the Silver S3 bucket."
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket_name" {
  description = "Name of the Gold S3 bucket."
  value       = aws_s3_bucket.gold.bucket
}

output "lambda_function_name" {
  description = "Name of the ingestion Lambda function."
  value       = aws_lambda_function.ingestion.function_name
}

output "lambda_function_arn" {
  description = "ARN of the ingestion Lambda function."
  value       = aws_lambda_function.ingestion.arn
}

output "lambda_exec_role_arn" {
  description = "ARN of the Lambda execution IAM role."
  value       = aws_iam_role.lambda_exec.arn
}

output "lambda_layer_arn" {
  description = "ARN of the published Lambda Layer (with version)."
  value       = aws_lambda_layer_version.deps.arn
}

output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge schedule rule."
  value       = aws_cloudwatch_event_rule.ingestion_schedule.arn
}

output "cloudwatch_log_group" {
  description = "CloudWatch Log Group name for the ingestion Lambda."
  value       = aws_cloudwatch_log_group.lambda.name
}
