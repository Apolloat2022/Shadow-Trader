###############################################################################
# paper_trader_lambda.tf
# Paper Trading Engine Lambda + IAM + EventBridge schedule.
###############################################################################

# ── Package paper_trader/ source ──────────────────────────────────────────────

data "archive_file" "paper_trader_code" {
  type        = "zip"
  source_dir  = "${path.module}/../paper_trader"
  output_path = "${path.module}/.build/paper_trader.zip"
}

# ── Lambda Layer (PyArrow) ────────────────────────────────────────────────────

resource "null_resource" "build_paper_trader_layer" {
  triggers = {
    req_hash = filemd5("${path.module}/../paper_trader/requirements.txt")
  }

  provisioner "local-exec" {
    interpreter = ["pwsh", "-Command"]
    command     = <<-EOT
      pip install `
        --requirement ${path.module}/../paper_trader/requirements.txt `
        --target ${path.module}/.build/paper_trader_layer/python `
        --platform manylinux2014_x86_64 `
        --implementation cp `
        --python-version 3.12 `
        --only-binary=:all: `
        --upgrade
    EOT
  }
}

data "archive_file" "paper_trader_layer" {
  type        = "zip"
  source_dir  = "${path.module}/.build/paper_trader_layer/python"
  output_path = "${path.module}/.build/paper_trader_layer.zip"
  depends_on  = [null_resource.build_paper_trader_layer]
}

resource "aws_lambda_layer_version" "paper_trader_deps" {
  layer_name          = "shadow-trader-paper-trader-deps"
  filename            = data.archive_file.paper_trader_layer.output_path
  source_code_hash    = data.archive_file.paper_trader_layer.output_base64sha256
  compatible_runtimes = [var.lambda_runtime]
}

# ── IAM Role for Paper Trader Lambda ─────────────────────────────────────────

resource "aws_iam_role" "paper_trader_exec" {
  name               = "shadow-trader-paper-trader-exec-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "paper_trader_permissions" {
  # S3: read Gold signals cache
  statement {
    sid     = "ReadGoldSignalsCache"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.gold.arn}/signals_cache/*"]
  }

  # DynamoDB: portfolio read/write
  statement {
    sid    = "PortfolioTableAccess"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.portfolio.arn]
  }

  # DynamoDB: trades append + query via GSI
  statement {
    sid    = "TradesTableAccess"
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:Query",
    ]
    resources = [
      aws_dynamodb_table.trades.arn,
      "${aws_dynamodb_table.trades.arn}/index/session-timestamp-index",
    ]
  }
}

resource "aws_iam_policy" "paper_trader_permissions" {
  name   = "shadow-trader-paper-trader-policy-${var.environment}"
  policy = data.aws_iam_policy_document.paper_trader_permissions.json
}

resource "aws_iam_role_policy_attachment" "paper_trader_permissions" {
  role       = aws_iam_role.paper_trader_exec.name
  policy_arn = aws_iam_policy.paper_trader_permissions.arn
}

resource "aws_iam_role_policy_attachment" "paper_trader_cw_logs" {
  role       = aws_iam_role.paper_trader_exec.name
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
}

# ── CloudWatch Log Group ──────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "paper_trader" {
  name              = "/aws/lambda/shadow-trader-paper-trader"
  retention_in_days = var.log_retention_days
}

# ── Paper Trader Lambda Function ──────────────────────────────────────────────

resource "aws_lambda_function" "paper_trader" {
  function_name    = "shadow-trader-paper-trader-${var.environment}"
  role             = aws_iam_role.paper_trader_exec.arn
  runtime          = var.lambda_runtime
  handler          = "engine.lambda_handler"
  filename         = data.archive_file.paper_trader_code.output_path
  source_code_hash = data.archive_file.paper_trader_code.output_base64sha256
  memory_size      = 256
  timeout          = 120

  layers = [aws_lambda_layer_version.paper_trader_deps.arn]

  environment {
    variables = {
      GOLD_S3_BUCKET           = aws_s3_bucket.gold.bucket
      DYNAMODB_PORTFOLIO_TABLE = aws_dynamodb_table.portfolio.name
      DYNAMODB_TRADES_TABLE    = aws_dynamodb_table.trades.name
      INITIAL_CASH_USD         = "100000"
      POSITION_SIZE_PCT        = "0.10"
    }
  }

  depends_on = [aws_cloudwatch_log_group.paper_trader]

  tags = { Component = "paper-trader" }
}

# ── EventBridge: Trigger 30 min after Gold completes (offset from top of hour) ─

resource "aws_cloudwatch_event_rule" "paper_trader_schedule" {
  name                = "shadow-trader-paper-trader-schedule-${var.environment}"
  description         = "Triggers Paper Trading Engine 30 min after the top of each hour."
  schedule_expression = "cron(30 * * * ? *)"   # :30 past every hour UTC
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "paper_trader_lambda" {
  rule      = aws_cloudwatch_event_rule.paper_trader_schedule.name
  target_id = "PaperTraderTarget"
  arn       = aws_lambda_function.paper_trader.arn

  input = jsonencode({ dry_run = false })
}

resource "aws_lambda_permission" "allow_eventbridge_paper_trader" {
  statement_id  = "AllowEventBridgePaperTrader"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.paper_trader.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.paper_trader_schedule.arn
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "paper_trader_function_name" {
  value = aws_lambda_function.paper_trader.function_name
}

output "portfolio_table_name" {
  value = aws_dynamodb_table.portfolio.name
}

output "trades_table_name" {
  value = aws_dynamodb_table.trades.name
}
