provider "aws" {
  region = "us-east-1"
}

# 1. BUCKETS: Medallion Architecture
resource "aws_s3_bucket" "market_data_bronze" { bucket = "shadow-trader-bronze-robin-2026" }
resource "aws_s3_bucket" "market_data_silver" { bucket = "shadow-trader-silver-robin-2026" }
resource "aws_s3_bucket" "market_data_gold"   { bucket = "shadow-trader-gold-robin-2026" }

# 2. LIFECYCLE: Keeping it lean
resource "aws_s3_bucket_lifecycle_configuration" "data_retention" {
  for_each = {
    "bronze" = aws_s3_bucket.market_data_bronze.id,
    "silver" = aws_s3_bucket.market_data_silver.id
  }
  bucket = each.value
  rule {
    id     = "archive-and-cleanup"
    status = "Enabled"
    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }
    expiration { days = 90 }
  }
}

# 3. IAM ROLE: Common Execution Role
resource "aws_iam_role" "lambda_exec_role" {
  name = "shadow_trader_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# 4. INGESTION LAMBDA (Bronze)
resource "aws_lambda_function" "crypto_ingestor" {
  filename      = "lambda_function.zip"
  function_name = "crypto_price_ingestor"
  role          = aws_iam_role.lambda_exec_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 60
  layers        = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:14"]
  environment {
    variables = {
      ALPHAVANTAGE_API_KEY = "YOUR_API_KEY"
      S3_BUCKET            = aws_s3_bucket.market_data_bronze.id
    }
  }
}

# 5. TRANSFORMER LAMBDA (Silver)
resource "aws_lambda_function" "silver_transformer" {
  filename      = "lambda_function.zip"
  function_name = "crypto_silver_transformer"
  role          = aws_iam_role.lambda_exec_role.arn
  handler       = "lambda_function.silver_handler"
  runtime       = "python3.12"
  timeout       = 60
  layers        = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:14"]
  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.market_data_bronze.id
      SILVER_BUCKET = aws_s3_bucket.market_data_silver.id
    }
  }
}

# 6. TRIGGER 1: Hourly Schedule for Ingestor
resource "aws_cloudwatch_event_rule" "hourly_fetch" {
  name                = "every-hour-crypto-fetch"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "fetch_target" {
  rule = aws_cloudwatch_event_rule.hourly_fetch.name
  arn  = aws_lambda_function.crypto_ingestor.arn
  input = jsonencode({ symbol = "BTC", market = "USD" })
}

resource "aws_lambda_permission" "allow_events" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.crypto_ingestor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.hourly_fetch.arn
}

# 7. TRIGGER 2: S3 Event for Transformer
resource "aws_lambda_permission" "allow_s3" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.silver_transformer.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.market_data_bronze.arn
}

resource "aws_s3_bucket_notification" "on_bronze_upload" {
  bucket = aws_s3_bucket.market_data_bronze.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.silver_transformer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/crypto_prices/"
  }
  depends_on = [aws_lambda_permission.allow_s3]
}
