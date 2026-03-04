###############################################################################
# s3.tf
# Creates the Bronze, Silver, and Gold S3 buckets for the Shadow Trader
# medallion architecture.
###############################################################################

locals {
  # Suffix buckets with environment to avoid global name collisions
  bronze_bucket = "${var.bronze_bucket_name}-${var.environment}"
  silver_bucket = "${var.silver_bucket_name}-${var.environment}"
  gold_bucket   = "${var.gold_bucket_name}-${var.environment}"
}

# ── Bronze Bucket (Raw Parquet from Lambda) ───────────────────────────────────

resource "aws_s3_bucket" "bronze" {
  bucket        = local.bronze_bucket
  force_destroy = var.environment != "prod" # Safety: protect prod data

  tags = { Layer = "bronze" }
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  count  = var.s3_lifecycle_expiration_days > 0 ? 1 : 0
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "expire-old-objects"
    status = "Enabled"
    expiration { days = var.s3_lifecycle_expiration_days }

    # Clean up incomplete multi-part uploads
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

# ── Silver Bucket (Cleaned Delta / Parquet) ───────────────────────────────────

resource "aws_s3_bucket" "silver" {
  bucket        = local.silver_bucket
  force_destroy = var.environment != "prod"

  tags = { Layer = "silver" }
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  count  = var.s3_lifecycle_expiration_days > 0 ? 1 : 0
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "expire-old-objects"
    status = "Enabled"
    expiration { days = var.s3_lifecycle_expiration_days }
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

# ── Gold Bucket (Features / Aggregations) ────────────────────────────────────

resource "aws_s3_bucket" "gold" {
  bucket        = local.gold_bucket
  force_destroy = var.environment != "prod"

  tags = { Layer = "gold" }
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
