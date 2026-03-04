###############################################################################
# cloudwatch.tf
# CloudWatch Log Group for the ingestion Lambda with configurable retention.
###############################################################################

resource "aws_cloudwatch_log_group" "lambda" {
  # Lambda auto-creates /aws/lambda/<name>; managing it here enforces retention.
  name              = "/aws/lambda/${var.lambda_function_name}"
  retention_in_days = var.log_retention_days

  tags = { Component = "ingestion" }
}

# ── CloudWatch Metric Alarm: Rate-Limit Warnings ──────────────────────────────
# Alerts when the [RATE_LIMIT] log pattern appears more than 3 times in 5 min.

resource "aws_cloudwatch_log_metric_filter" "rate_limit" {
  name           = "${var.lambda_function_name}-rate-limit-filter"
  pattern        = "[RATE_LIMIT]"
  log_group_name = aws_cloudwatch_log_group.lambda.name

  metric_transformation {
    name      = "RateLimitHits"
    namespace = "ShadowTrader/Ingestion"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "rate_limit" {
  alarm_name          = "${var.lambda_function_name}-rate-limit-alarm"
  alarm_description   = "Alpha Vantage API rate limit was hit during ingestion."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "RateLimitHits"
  namespace           = "ShadowTrader/Ingestion"
  period              = 300 # 5 minutes
  statistic           = "Sum"
  threshold           = 3
  treat_missing_data  = "notBreaching"
}

# ── CloudWatch Alarm: Lambda Errors ───────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.lambda_function_name}-error-alarm"
  alarm_description   = "Shadow Trader ingestion Lambda is throwing errors."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.ingestion.function_name
  }
}
