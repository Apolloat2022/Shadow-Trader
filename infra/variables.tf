###############################################################################
# variables.tf
# All configurable inputs for the Shadow Trader ingestion infrastructure.
###############################################################################

# ── General ─────────────────────────────────────────────────────────────────

variable "aws_region" {
  description = "AWS region to deploy all resources into."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev | staging | prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

# ── S3 ───────────────────────────────────────────────────────────────────────

variable "bronze_bucket_name" {
  description = "Name of the S3 Bronze (raw) data bucket."
  type        = string
  default     = "shadow-trader-bronze"
}

variable "silver_bucket_name" {
  description = "Name of the S3 Silver (cleaned) data bucket."
  type        = string
  default     = "shadow-trader-silver"
}

variable "gold_bucket_name" {
  description = "Name of the S3 Gold (feature/aggregation) data bucket."
  type        = string
  default     = "shadow-trader-gold"
}

variable "s3_lifecycle_expiration_days" {
  description = "Days after which objects in Bronze/Silver are expired (0 = disabled)."
  type        = number
  default     = 90
}

# ── Lambda ───────────────────────────────────────────────────────────────────

variable "lambda_function_name" {
  description = "Name of the ingestion Lambda function."
  type        = string
  default     = "shadow-trader-ingestion"
}

variable "lambda_memory_mb" {
  description = "Memory allocation for the Lambda function in MB."
  type        = number
  default     = 512
}

variable "lambda_timeout_seconds" {
  description = "Maximum execution time for the Lambda function in seconds."
  type        = number
  default     = 300
}

variable "lambda_runtime" {
  description = "Python runtime version for the Lambda function."
  type        = string
  default     = "python3.12"
}

# ── Alpha Vantage ────────────────────────────────────────────────────────────

variable "alphavantage_api_key" {
  description = "Alpha Vantage API key. Store in tfvars or pass via TF_VAR_ env var — never commit plaintext."
  type        = string
  sensitive   = true
}

variable "tickers" {
  description = "Comma-separated list of tickers to ingest (e.g. BTC,NVDA,ETH)."
  type        = string
  default     = "BTC,NVDA,ETH"
}

# ── EventBridge ──────────────────────────────────────────────────────────────

variable "schedule_expression" {
  description = "EventBridge schedule expression for the ingestion trigger."
  type        = string
  default     = "rate(1 hour)"
}

# ── CloudWatch ───────────────────────────────────────────────────────────────

variable "log_retention_days" {
  description = "Number of days to retain Lambda CloudWatch logs."
  type        = number
  default     = 30
}
