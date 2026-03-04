###############################################################################
# api_lambda.tf
# API Lambda function — serves HTTP requests from API Gateway.
###############################################################################

# ── Package the API code (all files under api/) ───────────────────────────────

data "archive_file" "api_code" {
  type        = "zip"
  source_dir  = "${path.module}/../api"
  output_path = "${path.module}/.build/api_function.zip"
}

# ── Lambda Layer for API deps (pyarrow etc.) ──────────────────────────────────

resource "null_resource" "build_api_layer" {
  triggers = {
    req_hash = filemd5("${path.module}/../api/requirements.txt")
  }

  provisioner "local-exec" {
    interpreter = ["pwsh", "-Command"]
    command     = <<-EOT
      pip install `
        --requirement ${path.module}/../api/requirements.txt `
        --target ${path.module}/.build/api_layer/python `
        --platform manylinux2014_x86_64 `
        --implementation cp `
        --python-version 3.12 `
        --only-binary=:all: `
        --upgrade
    EOT
  }
}

data "archive_file" "api_layer" {
  type        = "zip"
  source_dir  = "${path.module}/.build/api_layer/python"
  output_path = "${path.module}/.build/api_layer.zip"
  depends_on  = [null_resource.build_api_layer]
}

resource "aws_lambda_layer_version" "api_deps" {
  layer_name          = "shadow-trader-api-deps"
  filename            = data.archive_file.api_layer.output_path
  source_code_hash    = data.archive_file.api_layer.output_base64sha256
  compatible_runtimes = [var.lambda_runtime]
}

# ── IAM Role for API Lambda ───────────────────────────────────────────────────

resource "aws_iam_role" "api_lambda_exec" {
  name               = "shadow-trader-api-exec-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "api_s3_read" {
  statement {
    sid     = "AllowGoldSignalsCacheRead"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.gold.arn}/signals_cache/*"]
  }
  statement {
    sid       = "AllowGoldListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.gold.arn]
  }
  # Read portfolio and trades from DynamoDB
  statement {
    sid    = "ReadPortfolioTable"
    effect = "Allow"
    actions = ["dynamodb:GetItem"]
    resources = [aws_dynamodb_table.portfolio.arn]
  }
  statement {
    sid    = "ReadTradesTable"
    effect = "Allow"
    actions = ["dynamodb:Query"]
    resources = [
      aws_dynamodb_table.trades.arn,
      "${aws_dynamodb_table.trades.arn}/index/session-timestamp-index",
    ]
  }
}

resource "aws_iam_policy" "api_s3_read" {
  name   = "shadow-trader-api-s3-read-${var.environment}"
  policy = data.aws_iam_policy_document.api_s3_read.json
}

resource "aws_iam_role_policy_attachment" "api_s3_read" {
  role       = aws_iam_role.api_lambda_exec.name
  policy_arn = aws_iam_policy.api_s3_read.arn
}

resource "aws_iam_role_policy_attachment" "api_cw_logs" {
  role       = aws_iam_role.api_lambda_exec.name
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
}

# ── CloudWatch Log Group ──────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "api_lambda" {
  name              = "/aws/lambda/shadow-trader-api"
  retention_in_days = var.log_retention_days
}

# ── API Lambda Function ───────────────────────────────────────────────────────

resource "aws_lambda_function" "api" {
  function_name    = "shadow-trader-api-${var.environment}"
  role             = aws_iam_role.api_lambda_exec.arn
  runtime          = var.lambda_runtime
  handler          = "main.lambda_handler"
  filename         = data.archive_file.api_code.output_path
  source_code_hash = data.archive_file.api_code.output_base64sha256
  memory_size      = 256
  timeout          = 30

  layers = [aws_lambda_layer_version.api_deps.arn]

  environment {
    variables = {
      GOLD_S3_BUCKET           = aws_s3_bucket.gold.bucket
      SERVICE_VERSION          = "1.0.0"
      DYNAMODB_PORTFOLIO_TABLE = aws_dynamodb_table.portfolio.name
      DYNAMODB_TRADES_TABLE    = aws_dynamodb_table.trades.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.api_lambda]

  tags = { Component = "api" }
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "api_endpoint" {
  description = "Base URL for the Shadow Trader REST API."
  value       = aws_apigatewayv2_stage.default.invoke_url
}
