###############################################################################
# api_gateway.tf
# HTTP API Gateway (v2) — cheapest, lowest-latency option for Lambda-backed APIs.
###############################################################################

# ── HTTP API ──────────────────────────────────────────────────────────────────

resource "aws_apigatewayv2_api" "shadow_trader" {
  name          = "shadow-trader-api-${var.environment}"
  protocol_type = "HTTP"
  description   = "Shadow Trader signal and portfolio API"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "OPTIONS"]
    allow_headers = ["Content-Type", "Authorization"]
    max_age       = 300
  }
}

# ── Stage (auto-deploy) ───────────────────────────────────────────────────────

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.shadow_trader.id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_gateway.arn
  }
}

# ── Lambda Integration ────────────────────────────────────────────────────────

resource "aws_apigatewayv2_integration" "api_lambda" {
  api_id                 = aws_apigatewayv2_api.shadow_trader.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

# ── Routes ────────────────────────────────────────────────────────────────────

locals {
  api_routes = [
    "GET /health",
    "GET /portfolio",
    "GET /trades",
    "GET /signals/latest",
    "GET /signals/{ticker}",
    "OPTIONS /health",
    "OPTIONS /portfolio",
    "OPTIONS /trades",
    "OPTIONS /signals/latest",
    "OPTIONS /signals/{ticker}",
  ]
}

resource "aws_apigatewayv2_route" "api" {
  for_each = toset(local.api_routes)

  api_id    = aws_apigatewayv2_api.shadow_trader.id
  route_key = each.value
  target    = "integrations/${aws_apigatewayv2_integration.api_lambda.id}"
}

# ── Permission: Allow API Gateway to Invoke Lambda ────────────────────────────

resource "aws_lambda_permission" "allow_api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.shadow_trader.execution_arn}/*/*"
}

# ── CloudWatch Log Group for API Gateway ─────────────────────────────────────

resource "aws_cloudwatch_log_group" "api_gateway" {
  name              = "/aws/apigateway/shadow-trader-${var.environment}"
  retention_in_days = var.log_retention_days
}
