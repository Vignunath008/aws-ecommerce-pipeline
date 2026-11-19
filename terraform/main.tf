terraform {
  required_version = ">= 1.5"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.40" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "me" {}

locals {
  prefix         = "${var.project}-${var.env}"
  suffix         = data.aws_caller_identity.me.account_id
  raw_bucket     = "${local.prefix}-raw-${local.suffix}"
  curated_bucket = "${local.prefix}-curated-${local.suffix}"
  scripts_bucket = "${local.prefix}-scripts-${local.suffix}"
  glue_job_name  = "${local.prefix}-transform-orders"
}

# ---------------------------------------------------------------- S3
resource "aws_s3_bucket" "raw" {
  bucket = local.raw_bucket
}

resource "aws_s3_bucket" "curated" {
  bucket = local.curated_bucket
}

resource "aws_s3_bucket" "scripts" {
  bucket = local.scripts_bucket
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each = {
    raw     = aws_s3_bucket.raw.id
    curated = aws_s3_bucket.curated.id
    scripts = aws_s3_bucket.scripts.id
  }
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = {
    raw     = aws_s3_bucket.raw.id
    curated = aws_s3_bucket.curated.id
    scripts = aws_s3_bucket.scripts.id
  }
  bucket = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ---------------------------------------------------------------- Lambda
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/validate_and_trigger.py"
  output_path = "${path.module}/build/validate_and_trigger.zip"
}

resource "aws_iam_role" "lambda" {
  name = "${local.prefix}-lambda-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${local.prefix}-lambda-policy"
  role   = aws_iam_role.lambda.id
  policy = templatefile("${path.module}/../iam/lambda_policy.json", { raw_bucket = local.raw_bucket })
}

resource "aws_lambda_function" "validate" {
  function_name    = "${local.prefix}-validate-and-trigger"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "validate_and_trigger.lambda_handler"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 60
  memory_size      = 256
  environment {
    variables = {
      VALIDATED_PREFIX  = "validated/"
      QUARANTINE_PREFIX = "quarantine/"
    }
  }
}

resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.validate.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}

resource "aws_s3_bucket_notification" "raw_incoming" {
  bucket = aws_s3_bucket.raw.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.validate.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "incoming/"
    filter_suffix       = ".csv"
  }
  depends_on = [aws_lambda_permission.s3_invoke]
}

# ---------------------------------------------------------------- Glue
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue/transform_orders.py"
  source = "${path.module}/../glue/transform_orders.py"
  etag   = filemd5("${path.module}/../glue/transform_orders.py")
}

resource "aws_iam_role" "glue" {
  name = "${local.prefix}-glue-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue" {
  name = "${local.prefix}-glue-policy"
  role = aws_iam_role.glue.id
  policy = templatefile("${path.module}/../iam/glue_policy.json", {
    raw_bucket     = local.raw_bucket
    curated_bucket = local.curated_bucket
    scripts_bucket = local.scripts_bucket
  })
}

resource "aws_glue_job" "transform_orders" {
  name              = local.glue_job_name
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.bucket}/tmp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }
}

# ---------------------------------------------------------------- Redshift
resource "aws_iam_role" "redshift_s3_read" {
  name = "${local.prefix}-redshift-s3-read"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"] }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3_read" {
  name   = "${local.prefix}-redshift-s3-read"
  role   = aws_iam_role.redshift_s3_read.id
  policy = templatefile("${path.module}/../iam/redshift_s3_read_policy.json", { curated_bucket = local.curated_bucket })
}

resource "aws_redshiftserverless_namespace" "main" {
  count               = var.create_redshift ? 1 : 0
  namespace_name      = "${local.prefix}-ns"
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_user
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3_read.arn]
  default_iam_role_arn = aws_iam_role.redshift_s3_read.arn

  lifecycle {
    precondition {
      condition     = var.redshift_admin_password != null
      error_message = "Set redshift_admin_password in terraform.tfvars (or set create_redshift = false)."
    }
  }
}

resource "aws_redshiftserverless_workgroup" "main" {
  count               = var.create_redshift ? 1 : 0
  namespace_name      = aws_redshiftserverless_namespace.main[0].namespace_name
  workgroup_name      = "${local.prefix}-wg"
  base_capacity       = 8
  publicly_accessible = false
}

# ---------------------------------------------------------------- Airflow (MWAA) policy
# Attach this to your MWAA execution role (MWAA itself is not created here).
resource "aws_iam_policy" "airflow" {
  name = "${local.prefix}-airflow-policy"
  policy = templatefile("${path.module}/../iam/airflow_policy.json", {
    raw_bucket    = local.raw_bucket
    glue_job_name = local.glue_job_name
  })
}
