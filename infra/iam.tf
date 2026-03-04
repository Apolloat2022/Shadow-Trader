###############################################################################
# iam.tf
# Lambda execution role with least-privilege S3 and CloudWatch permissions.
###############################################################################

# ── Trust Policy ─────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    sid     = "AllowLambdaAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# ── Execution Role ────────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda_exec" {
  name               = "${var.lambda_function_name}-exec-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = { Purpose = "lambda-execution" }
}

# ── Policy: CloudWatch Logs ───────────────────────────────────────────────────

data "aws_iam_policy_document" "cloudwatch_logs" {
  statement {
    sid    = "AllowCloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${var.aws_region}:*:log-group:/aws/lambda/${var.lambda_function_name}",
      "arn:aws:logs:${var.aws_region}:*:log-group:/aws/lambda/${var.lambda_function_name}:*",
    ]
  }
}

resource "aws_iam_policy" "cloudwatch_logs" {
  name        = "${var.lambda_function_name}-cw-logs-policy"
  description = "Allow Lambda to write logs to CloudWatch."
  policy      = data.aws_iam_policy_document.cloudwatch_logs.json
}

resource "aws_iam_role_policy_attachment" "cloudwatch_logs" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
}

# ── Policy: S3 Bronze Write (Least Privilege) ─────────────────────────────────

data "aws_iam_policy_document" "s3_bronze_write" {
  # PutObject: write Parquet files
  statement {
    sid    = "AllowBronzePutObject"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectAcl",
    ]
    resources = ["${aws_s3_bucket.bronze.arn}/*"]
  }

  # ListBucket: required by boto3 to verify bucket existence before writing
  statement {
    sid       = "AllowBronzeListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.bronze.arn]
  }
}

resource "aws_iam_policy" "s3_bronze_write" {
  name        = "${var.lambda_function_name}-s3-bronze-write-policy"
  description = "Least-privilege write access to the Bronze S3 bucket."
  policy      = data.aws_iam_policy_document.s3_bronze_write.json
}

resource "aws_iam_role_policy_attachment" "s3_bronze_write" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.s3_bronze_write.arn
}
