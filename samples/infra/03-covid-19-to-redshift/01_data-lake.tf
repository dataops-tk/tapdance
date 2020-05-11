module "data_lake" {
  # BOILERPLATE HEADER (NO NEED TO CHANGE):
  source        = "git::https://github.com/slalom-ggp/dataops-infra//catalog/aws/data-lake?ref=master"
  name_prefix   = local.name_prefix
  environment   = module.env.environment
  resource_tags = local.resource_tags

  # CONFIGURE HERE:


  /*
  # OPTIONALLY, COPY-PASTE ADDITIONAL SETTINGS FROM BELOW:

  admin_cidr       = []
  app_cidr         = ["0.0.0.0/0"]
  lambda_python_source = "${path.module}/python/fn_lambda_logger"
  s3_triggers = {
    "fn_lambda_logger" = {
      triggering_path     = "uploads/*"
      lambda_handler      = "main.lambda_handler"
      environment_vars    = {}
      environment_secrets = {}
    }
  }

  */
}
output "data_lake_summary" { value = module.data_lake.summary }
