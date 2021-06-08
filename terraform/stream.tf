# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

locals {
  kinesis_stream_name             = "autoscaling-kinesis-stream"
  kinesis_stream_retention_period = 168 # hours. Must use 168 hours (7 days) max for Disaster Recovery
  kinesis_stream_shard_count      = 1 # Starting shard count, autoscaling will right-size this
}

################################
# Kinesis Data Stream
################################
resource aws_kinesis_stream autoscaling_kinesis_stream {
  name             = local.kinesis_stream_name
  shard_count      = local.kinesis_stream_shard_count
  retention_period = local.kinesis_stream_retention_period

  shard_level_metrics = [
    "IncomingBytes",
  ]

  lifecycle {
    ignore_changes = [
      shard_count, # Kinesis autoscaling will change the shard count outside of terraform
    ]
  }
}

######################################################
# Kinesis Data Stream Scaling Alarms
######################################################
resource aws_cloudwatch_metric_alarm kinesis_scale_up {
  alarm_name                = "${aws_kinesis_stream.autoscaling_kinesis_stream.name}-scale-up"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = local.kinesis_scale_up_evaluation_period    # Defined in scale.tf
  datapoints_to_alarm       = local.kinesis_scale_up_datapoints_required  # Defined in scale.tf
  threshold                 = local.kinesis_scale_up_threshold            # Defined in scale.tf
  alarm_description         = "Stream throughput has gone above the scale up threshold"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.kinesis_scaling_sns_topic.arn]

  metric_query {
    id         = "s1"
    label      = "ShardCount"
    expression = aws_kinesis_stream.autoscaling_kinesis_stream.shard_count
  }

  metric_query {
    id    = "m1"
    label = "IncomingBytes"
    metric {
      metric_name = "IncomingBytes"
      namespace   = "AWS/Kinesis"
      period      = local.kinesis_period_secs
      stat        = "Sum"
      dimensions = {
        StreamName = aws_kinesis_stream.autoscaling_kinesis_stream.name
      }
    }
  }

  metric_query {
    id    = "m2"
    label = "IncomingRecords"
    metric {
      metric_name = "IncomingRecords"
      namespace   = "AWS/Kinesis"
      period      = local.kinesis_period_secs
      stat        = "Sum"
      dimensions = {
        StreamName = aws_kinesis_stream.autoscaling_kinesis_stream.name
      }
    }
  }

  metric_query {
    id         = "e1"
    label      = "FillMissingDataPointsWithZeroForIncomingBytes"
    expression = "FILL(m1,0)"
  }

  metric_query {
    id         = "e2"
    label      = "FillMissingDataPointsWithZeroForIncomingRecords"
    expression = "FILL(m2,0)"
  }

  metric_query {
    id         = "e3"
    label      = "IncomingBytesUsageFactor"
    expression = "e1/(1024*1024*60*${local.kinesis_period_mins}*s1)"
  }

  metric_query {
    id         = "e4"
    label      = "IncomingRecordsUsageFactor"
    expression = "e2/(1000*60*${local.kinesis_period_mins}*s1)"
  }

  metric_query {
    id          = "e5"
    label       = "MaxIncomingUsageFactor"
    expression  = "MAX([e3,e4])" # Take the highest usage factor between bytes/sec and records/sec
    return_data = true
  }

  lifecycle {
    ignore_changes = [
      tags["LastScaledTimestamp"] # A tag that is updated every time Kinesis autoscales the stream
    ]
  }

  depends_on = [
    aws_lambda_function.kinesis_scaling_function # The lambda function needs to be updated before the alarms. A scenario where
                                                 # this matters is changing the scaling thresholds which are baked into the scaling
                                                 # lambda environment variables. If the alarms are updated first it could trigger
                                                 # the scaling lambda before terraform gives the lambda the new thresholds, resulting
                                                 # in these scaling alarms being rebuilt by the alarm using the old thresholds.
  ]
}

resource aws_cloudwatch_metric_alarm kinesis_scale_down {
  alarm_name                = "${aws_kinesis_stream.autoscaling_kinesis_stream.name}-scale-down"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = local.kinesis_scale_down_evaluation_period                                                                      # Defined in scale.tf
  datapoints_to_alarm       = local.kinesis_scale_down_datapoints_required                                                                    # Defined in scale.tf
  threshold                 = aws_kinesis_stream.autoscaling_kinesis_stream.shard_count == 1 ? -1 : local.kinesis_scale_down_threshold        # Defined in scale.tf
  alarm_description         = "Stream throughput has gone below the scale down threshold"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.kinesis_scaling_sns_topic.arn]

  metric_query {
    id         = "s1"
    label      = "ShardCount"
    expression = aws_kinesis_stream.autoscaling_kinesis_stream.shard_count
  }

  metric_query {
    id         = "s2"
    label      = "IteratorAgeMinutesToBlockScaledowns"
    expression = local.kinesis_scale_down_min_iter_age_mins
  }

  metric_query {
    id    = "m1"
    label = "IncomingBytes"
    metric {
      metric_name = "IncomingBytes"
      namespace   = "AWS/Kinesis"
      period      = local.kinesis_period_secs
      stat        = "Sum"
      dimensions = {
        StreamName = aws_kinesis_stream.autoscaling_kinesis_stream.name
      }
    }
  }

  metric_query {
    id    = "m2"
    label = "IncomingRecords"
    metric {
      metric_name = "IncomingRecords"
      namespace   = "AWS/Kinesis"
      period      = local.kinesis_period_secs
      stat        = "Sum"
      dimensions = {
        StreamName = aws_kinesis_stream.autoscaling_kinesis_stream.name
      }
    }
  }

  metric_query {
    id    = "m3"
    label = "GetRecords.IteratorAgeMilliseconds"
    metric {
      metric_name = "GetRecords.IteratorAgeMilliseconds"
      namespace   = "AWS/Kinesis"
      period      = local.kinesis_period_secs
      stat        = "Maximum"
      dimensions = {
        StreamName = aws_kinesis_stream.autoscaling_kinesis_stream.name
      }
    }
  }

  metric_query {
    id         = "e1"
    label      = "FillMissingDataPointsWithZeroForIncomingBytes"
    expression = "FILL(m1,0)"
  }

  metric_query {
    id         = "e2"
    label      = "FillMissingDataPointsWithZeroForIncomingRecords"
    expression = "FILL(m2,0)"
  }

  metric_query {
    id         = "e3"
    label      = "IncomingBytesUsageFactor"
    expression = "e1/(1024*1024*60*${local.kinesis_period_mins}*s1)"
  }

  metric_query {
    id         = "e4"
    label      = "IncomingRecordsUsageFactor"
    expression = "e2/(1000*60*${local.kinesis_period_mins}*s1)"
  }

  metric_query {
    id         = "e5"
    label      = "IteratorAgeAdjustedFactor"
    expression = "(FILL(m3,0)/1000/60)*(${local.kinesis_scale_down_threshold}/s2)" # We want to block scaledowns when IterAge is > 60 mins, multiply IterAge so 60 mins = <alarmThreshold>
  }

  metric_query {
    id          = "e6"
    label       = "MaxIncomingUsageFactor"
    expression  = "MAX([e3,e4,e5])" # Take the highest usage factor between bytes/sec, records/sec, and adjusted iterator age
    return_data = true
  }

  lifecycle {
    ignore_changes = [
      tags["LastScaledTimestamp"] # A tag that is updated every time Kinesis autoscales the stream
    ]
  }

  depends_on = [
    aws_lambda_function.kinesis_scaling_function # The lambda function needs to be updated before the alarms. A scenario where
                                                 # this matters is changing the scaling thresholds which are baked into the scaling
                                                 # lambda environment variables. If the alarms are updated first it could trigger
                                                 # the scaling lambda before terraform gives the lambda the new thresholds, resulting
                                                 # in these scaling alarms being rebuilt by the alarm using the old thresholds.
  ]
}
