###############################################################################
# dynamodb.tf
# DynamoDB tables for the Paper Trading Engine virtual portfolio state.
###############################################################################

# ── Portfolio Table (current state: cash + open positions) ────────────────────
# Read/write pattern: GetItem + PutItem on session_id (PK only, small item)
# Billing: PAY_PER_REQUEST (no capacity to manage — perfect for hourly lambda)

resource "aws_dynamodb_table" "portfolio" {
  name         = "shadow-trader-portfolio-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "session_id"

  attribute {
    name = "session_id"
    type = "S"
  }

  point_in_time_recovery { enabled = true }

  tags = { Component = "paper-trader" }
}

# ── Trades Table (append-only log of all executed trades) ─────────────────────
# Read pattern: Query by session_id (GSI), descending by timestamp

resource "aws_dynamodb_table" "trades" {
  name         = "shadow-trader-trades-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "trade_id"
  range_key    = "timestamp"

  attribute {
    name = "trade_id"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "S"
  }

  attribute {
    name = "session_id"
    type = "S"
  }

  # GSI: query all trades for a session ordered by time
  global_secondary_index {
    name            = "session-timestamp-index"
    hash_key        = "session_id"
    range_key       = "timestamp"
    projection_type = "ALL"
  }

  point_in_time_recovery { enabled = true }

  tags = { Component = "paper-trader" }
}
