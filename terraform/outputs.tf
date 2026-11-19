output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "curated_path" {
  value = "s3://${aws_s3_bucket.curated.bucket}/orders/"
}

output "glue_job_name" {
  value = aws_glue_job.transform_orders.name
}

output "lambda_function_name" {
  value = aws_lambda_function.validate.function_name
}

output "redshift_iam_role_arn" {
  value = aws_iam_role.redshift_s3_read.arn
}

output "redshift_endpoint" {
  value = var.create_redshift ? aws_redshiftserverless_workgroup.main[0].endpoint : null
}

output "airflow_policy_arn" {
  value = aws_iam_policy.airflow.arn
}
