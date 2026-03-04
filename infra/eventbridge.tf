###############################################################################
# eventbridge.tf
# Schedules the ingestion Lambda to run on a recurring basis via EventBridge.
###############################################################################

# ── EventBridge Rule ──────────────────────────────────────────────────────────

resource "aws_cloudwatch_event_rule" "ingestion_schedule" {
  name                = "${var.lambda_function_name}-schedule"
  description         = "Triggers Shadow Trader ingestion Lambda on a schedule."
  schedule_expression = var.schedule_expression
  state               = "ENABLED"
}

# ── EventBridge Target (Lambda) ───────────────────────────────────────────────

resource "aws_cloudwatch_event_target" "ingestion_lambda" {
  rule      = aws_cloudwatch_event_rule.ingestion_schedule.name
  target_id = "IngestLambdaTarget"
  arn       = aws_lambda_function.ingestion.arn

  # Pass an optional payload to override tickers at schedule time
  input = jsonencode({
    tickers = split(",", var.tickers)
  })
}

# ── Permission: Allow EventBridge to Invoke Lambda ────────────────────────────

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingestion_schedule.arn
}
