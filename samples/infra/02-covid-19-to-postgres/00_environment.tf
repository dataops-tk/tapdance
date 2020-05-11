# STANDARD ENVIRONMENT DEFINITION
# NO NEED TO MODIFY THIS FILE

data "local_file" "config_yml" { filename = "${path.module}/infra-config.yml" }
locals {
  config             = yamldecode(data.local_file.config_yml.content)
  secrets_folder     = "${path.module}/../../.secrets"
  secrets_file_path  = "${local.secrets_folder}/aws-secrets-manager-secrets.yml"
  aws_creds_file     = "${local.secrets_folder}/aws-credentials"
  aws_creds_validate = length(file(local.aws_creds_file)) # Confirm file exists
  project_shortname  = local.config["project_shortname"]
  name_prefix        = "${local.project_shortname}-"
  aws_region         = local.config["aws_region"]
  resource_tags      = local.config["resource_tags"]
}

provider "aws" {
  version                 = "~> 2.10"
  region                  = local.aws_region
  profile                 = "default"
  shared_credentials_file = local.aws_creds_file
  # shared_credentials_file = "not-here"
}

output "env_summary" { value = module.env.summary }
module "env" {
  source         = "git::https://github.com/slalom-ggp/dataops-infra//catalog/aws/environment?ref=master"
  name_prefix    = local.name_prefix
  aws_region     = local.aws_region
  resource_tags  = local.resource_tags
  secrets_folder = local.secrets_folder
}
