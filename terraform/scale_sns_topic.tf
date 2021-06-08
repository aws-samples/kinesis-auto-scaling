# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

locals {
  kinesis_scaling_sns_topic_name = "kinesis-scaling-topic"
}

resource aws_sns_topic kinesis_scaling_sns_topic {
  name = local.kinesis_scaling_sns_topic_name
}

resource aws_sns_topic_subscription kinesis_scaling_sns_topic_subscription {
  topic_arn = aws_sns_topic.kinesis_scaling_sns_topic.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.kinesis_scaling_function.arn
}

resource aws_lambda_permission kinesis_scaling_sns_topic_permission {
  statement_id  = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.kinesis_scaling_function.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.kinesis_scaling_sns_topic.arn
}
