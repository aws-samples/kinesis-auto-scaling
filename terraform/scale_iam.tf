# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

locals {
  kinesis_scaling_lambda_role_name   = "${local.kinesis_scaling_function_name}-role"
  kinesis_scaling_lambda_policy_name = "${local.kinesis_scaling_function_name}-policy"
}

##########################################
# IAM Role for Kinesis Auto-Scaling Lambda
##########################################
resource aws_iam_role kinesis_scaling_lambda_role {
  name               = local.kinesis_scaling_lambda_role_name
  assume_role_policy = data.aws_iam_policy_document.kinesis_scaling_lambda_trust_policy_document.json
  tags               = {
      Name = local.kinesis_scaling_lambda_role_name
  }
}

data aws_iam_policy_document kinesis_scaling_lambda_trust_policy_document {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

##################################
# IAM Policy for Stream Handler Lambda
##################################
data aws_iam_policy_document kinesis_scaling_lambda_policy_document {
  statement {
    sid       = "AllowCreateCloudWatchAlarms"
    effect    = "Allow"
    resources = ["*"]

    actions = [
      "cloudwatch:DescribeAlarms",
      "cloudwatch:GetMetricData",
      "cloudwatch:ListMetrics",
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:PutMetricData",
      "cloudwatch:ListTagsForResource",
      "cloudwatch:SetAlarmState",
      "cloudwatch:TagResource"
    ]
  }

  statement {
    sid       = "AllowLoggingToCloudWatch"
    effect    = "Allow"
    resources = ["*"]

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
  }

  statement {
    sid       = "AllowReadFromKinesis"
    effect    = "Allow"
    resources = ["arn:aws:kinesis:${local.region}:${local.account_id}:stream/*"]

    actions = [
      "kinesis:DescribeStreamSummary",
      "kinesis:AddTagsToStream",
      "kinesis:ListTagsForStream",
      "kinesis:UpdateShardCount",
    ]
  }

  statement {
    sid       = "AllowPublishToSNS"
    effect    = "Allow"
    resources = ["arn:aws:sns:${local.region}:${local.account_id}:*"]

    actions = [
      "sns:Publish",
    ]
  }

  statement {
    sid       = "AllowChangeFunctionConcurrencyForLambda"
    effect    = "Allow"
    resources = ["arn:aws:lambda:${local.region}:${local.account_id}:function:*"]

    actions = [
      "lambda:PutFunctionConcurrency",
      "lambda:DeleteFunctionConcurrency"
    ]
  }
}

resource aws_iam_policy kinesis_scaling_lambda_policy {
  name        = local.kinesis_scaling_lambda_policy_name
  path        = "/"
  description = "Policy for Central Logging Kinesis Auto-Scaling Lambda"
  policy      = data.aws_iam_policy_document.kinesis_scaling_lambda_policy_document.json
}

##################################
# Attach Lambda Policy to Role
##################################
resource aws_iam_role_policy_attachment attach_kinesis_scaling_lambda_policy {
  role       = aws_iam_role.kinesis_scaling_lambda_role.name
  policy_arn = aws_iam_policy.kinesis_scaling_lambda_policy.arn
}
