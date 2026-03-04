###############################################################################
# providers.tf
# Configures the AWS provider and the optional S3 remote backend for state.
###############################################################################

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }

  # ── Remote State (S3 + DynamoDB locking) ────────────────────────────────
  # Uncomment and fill in once you have created the state bucket & lock table.
  # backend "s3" {
  #   bucket         = "shadow-trader-terraform-state"
  #   key            = "ingestion/terraform.tfstate"
  #   region         = var.aws_region          # Note: vars cannot be used here;
  #   dynamodb_table = "shadow-trader-tf-lock"  # hard-code region & table name.
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "shadow-trader"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
