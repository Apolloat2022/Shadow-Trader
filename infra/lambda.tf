###############################################################################
# lambda.tf
# Packages and deploys the ingestion Lambda function and its dependency layer.
###############################################################################

# ── Step 1: Package the Lambda function code ──────────────────────────────────

data "archive_file" "lambda_code" {
  type        = "zip"
  source_file = "${path.module}/../lambda_function.py"
  output_path = "${path.module}/.build/lambda_function.zip"
}

# ── Step 2: Build the Lambda Layer (pip install) ───────────────────────────────
# The null_resource triggers a rebuild whenever requirements.txt changes.
# Run build_layer.ps1 (Windows) before `terraform apply`.
# The layer zip is expected at .build/layer.zip after the script runs.

resource "null_resource" "build_layer" {
  triggers = {
    requirements_hash = filemd5("${path.module}/../requirements.txt")
  }

  provisioner "local-exec" {
    # Cross-platform: PowerShell works on Windows, macOS, and Linux.
    interpreter = ["pwsh", "-Command"]
    command     = "${path.module}/../build_layer.ps1"
  }
}

data "archive_file" "lambda_layer" {
  type        = "zip"
  source_dir  = "${path.module}/.build/layer/python"
  output_path = "${path.module}/.build/layer.zip"

  depends_on = [null_resource.build_layer]
}

# ── Step 3: Publish the Lambda Layer version ──────────────────────────────────

resource "aws_lambda_layer_version" "deps" {
  layer_name          = "${var.lambda_function_name}-deps"
  filename            = data.archive_file.lambda_layer.output_path
  source_code_hash    = data.archive_file.lambda_layer.output_base64sha256
  compatible_runtimes = [var.lambda_runtime]

  description = "Shadow Trader ingestion dependencies: pandas, pyarrow, requests"
}

# ── Step 4: Deploy the Lambda function ────────────────────────────────────────

resource "aws_lambda_function" "ingestion" {
  function_name    = var.lambda_function_name
  role             = aws_iam_role.lambda_exec.arn
  runtime          = var.lambda_runtime
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.lambda_code.output_path
  source_code_hash = data.archive_file.lambda_code.output_base64sha256
  memory_size      = var.lambda_memory_mb
  timeout          = var.lambda_timeout_seconds

  layers = [aws_lambda_layer_version.deps.arn]

  environment {
    variables = {
      ALPHAVANTAGE_API_KEY = var.alphavantage_api_key
      TARGET_S3_BUCKET     = aws_s3_bucket.bronze.bucket
      TICKERS              = var.tickers
    }
  }

  # Ensure the log group is created before the function so retention is applied
  depends_on = [aws_cloudwatch_log_group.lambda]

  tags = { Component = "ingestion" }
}
