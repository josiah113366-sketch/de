output "s3_bucket_name" {
  description = "s3 bucket name by airflow"
  value       = "local.airflow_bucket_name"
}

output "silver_glue_database_name" {
  description = "s3 bucket name by airflow"
  value       = "local.airflow_bucket_name"
}
output "silver_glue_table_name" {
  description = "s3 bucket name by airflow"
  value       = "local.airflow_bucket_name"
}
output "silver_s3_bucket_name" {
  description = "s3 bucket name by airflow"
  value       = "s3://${var.silver_bucket_name}/silver/"
}