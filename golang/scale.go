// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//go:generate mockgen -destination=./mocks/aws/mock_cloudwatch.go -package=mocks github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface CloudWatchAPI
//go:generate mockgen -destination=./mocks/aws/mock_kinesis.go -package=mocks github.com/aws/aws-sdk-go/service/kinesis/kinesisiface KinesisAPI
//go:generate mockgen -destination=./mocks/aws/mock_sns.go -package=mocks github.com/aws/aws-sdk-go/service/sns/snsiface SNSAPI
//go:generate mockgen -destination=./mocks/aws/mock_lambda.go -package=mocks github.com/aws/aws-sdk-go/service/lambda/lambdaiface LambdaAPI
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	lambdaService "github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/lambda/lambdaiface"
	"github.com/sirupsen/logrus"
)

// Declare CloudWatch client
var svcCloudWatch cloudwatchiface.CloudWatchAPI

// Declare Kinesis Client
var svcKinesis kinesisiface.KinesisAPI

// Declare the Lambda Client
var lambdaClient lambdaiface.LambdaAPI

var logs *logrus.Logger
var logger *logrus.Entry
var version = os.Getenv("version")
var deploymentRing = os.Getenv("deploymentRing")
var throttleRetryMin, throttleRetryMax, throttleRetryCount int64
var functionName string

const (
	fatalErrorMetric string = "FATAL_ERROR_KINESIS_SCALING"
)

// Initialize the variables
func init() {
	logs = logrus.New()
	logs.SetFormatter(&logrus.JSONFormatter{})
	logs.SetReportCaller(true)
	logLevel, err := logrus.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		logs.SetLevel(logrus.InfoLevel)
	} else {
		logs.SetLevel(logLevel)
	}
	logger = logs.WithField("version", version).WithField("deploymentRing", deploymentRing)
	functionName = os.Getenv("AWS_LAMBDA_FUNCTION_NAME")
}

// This lambda will be triggered by Scale-Up and Scale-Down cloudwatch alarms through an SNS topic.
// Parse SNS Message to retrieve the Alarm Information along with alarm name that triggered this Lambda.
// List tags for the alarm  to figure out the scale-up and scale-down alarm names and scaling action for this invocation.
// List tags for stream, validate scaling attempt and update the stream with new shard count.
// Update Kinesis tag with timestamp.
// Update alarm metrics with new shard count.
// Update alarm states to INSUFFICIENT_DATA.
// Update Alarm Tags.
func handleRequest(_ context.Context, snsEvent events.SNSEvent) {
	var periodMins int64
	var alarmInformation aws.JSONValue
	var scaleUpAlarmName string
	var scaleDownAlarmName string
	var lastScaledTimestamp string
	var evaluationPeriodScaleUp, evaluationPeriodScaleDown int64
	var datapointsRequiredScaleUp, datapointsRequiredScaleDown int64
	var upThreshold, downThreshold float64
	var scaleDownMinIterAgeMins int64
	var minShardCount int64
	var dryRun = true
	// Note: Investigate envconfig (https://github.com/kelseyhightower/envconfig) to simplify this environment variable section to have less boilerplate.
	periodMins, err := strconv.ParseInt(os.Getenv("SCALE_PERIOD_MINS"), 10, 64)
	if err != nil {
		// Default scaling period in minutes.
		periodMins = 5
		logMessage := "Error reading the SCALE_PERIOD_MINS environment variable. Stream will scale and update the scale-up alarm with default scaling period of 5 minute(s)."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	evaluationPeriodScaleUp, err = strconv.ParseInt(os.Getenv("SCALE_UP_EVALUATION_PERIOD"), 10, 64)
	if err != nil {
		// Default scale-up alarm evaluation period.
		evaluationPeriodScaleUp = 25 / periodMins // X mins / Y minute period = total data points
		logMessage := "Error reading the SCALE_UP_EVALUATION_PERIOD environment variable. Stream will scale and update the scale-up alarm with default scale-up evaluation period of 25 minute(s)."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	evaluationPeriodScaleDown, err = strconv.ParseInt(os.Getenv("SCALE_DOWN_EVALUATION_PERIOD"), 10, 64)
	if err != nil {
		// Default scale-down alarm evaluation period.
		evaluationPeriodScaleDown = 300 / periodMins // X mins / Y minute period = total data points
		logMessage := "Error reading the SCALE_DOWN_EVALUATION_PERIOD environment variable. Stream will scale and update the scale-down alarm with default scale-down evaluation period of 300 minute(s)."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	datapointsRequiredScaleUp, err = strconv.ParseInt(os.Getenv("SCALE_UP_DATAPOINTS_REQUIRED"), 10, 64)
	if err != nil {
		// Default scale-up alarm datapoints required.
		datapointsRequiredScaleUp = 25 / periodMins // X mins / Y minute period = total data points
		logMessage := "Error reading the SCALE_UP_DATAPOINTS_REQUIRED environment variable. Stream will scale and update the scale-up alarm with default scale-up datapoints required."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	datapointsRequiredScaleDown, err = strconv.ParseInt(os.Getenv("SCALE_DOWN_DATAPOINTS_REQUIRED"), 10, 64)
	if err != nil {
		// Default scale-down alarm datapoints required.
		datapointsRequiredScaleDown = 285 / periodMins // X mins / Y minute period = total data points
		logMessage := "Error reading the SCALE_DOWN_DATAPOINTS_REQUIRED environment variable. Stream will scale and update the scale-down alarm with default scale-up datapoints required."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	scaleDownMinIterAgeMins, err = strconv.ParseInt(os.Getenv("SCALE_DOWN_MIN_ITER_AGE_MINS"), 10, 64)
	if err != nil {
		// If the streams max iterator age is above this, then the stream will not scale down (we need all the shards/lambdas to clear the backlog, only scale down when it's cleared)
		scaleDownMinIterAgeMins = 30
		logMessage := "Error reading the SCALE_DOWN_MIN_ITER_AGE_MINS environment variable. Stream will default to 30 minutes."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	upThreshold, err = strconv.ParseFloat(os.Getenv("SCALE_UP_THRESHOLD"), 64)
	if err != nil {
		// Default scale-up threshold.
		upThreshold = 0.25
		logMessage := "Error reading the SCALE_UP_THRESHOLD environment variable. Stream will scale and update the scale-up alarm with default scale-up threshold of 0.25"
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	downThreshold, err = strconv.ParseFloat(os.Getenv("SCALE_DOWN_THRESHOLD"), 64)
	if err != nil {
		// Default scale-down threshold.
		downThreshold = 0.075
		logMessage := "Error reading the SCALE_DOWN_THRESHOLD environment variable. Stream will scale and update the scale-down alarm with default scale-down threshold of 0.075"
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	throttleRetryMin, err = strconv.ParseInt(os.Getenv("THROTTLE_RETRY_MIN_SLEEP"), 10, 64)
	if err != nil {
		// Default throttle retry floor value for generating random integer.
		throttleRetryMin = 1
		logMessage := "Error reading the THROTTLE_RETRY_MIN environment variable. The retries will still happen if required with a floor value of 1 for the generating random time.sleep value."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	throttleRetryMax, err = strconv.ParseInt(os.Getenv("THROTTLE_RETRY_MAX_SLEEP"), 10, 64)
	if err != nil {
		// Default throttle retry ceiling value for generating random integer.
		throttleRetryMax = 3
		logMessage := "Error reading the THROTTLE_RETRY_MAX environment variable. The retries will still happen if required with a ceiling value of 3 for the generating random time.sleep value."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	throttleRetryCount, err = strconv.ParseInt(os.Getenv("THROTTLE_RETRY_COUNT"), 10, 64)
	if err != nil {
		// Default throttle retry ceiling value for generating random integer.
		throttleRetryCount = 30
		logMessage := "Error reading the THROTTLE_RETRY_COUNT environment variable. The API will be retried for a maximum number of 30 times."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	minShardCount, err = strconv.ParseInt(os.Getenv("MIN_SHARD_COUNT"), 10, 64)
	if err != nil {
		// Default minimum shard count
		minShardCount = 1
		logMessage := "Error reading the MIN_SHARD_COUNT environment variable. Stream will scale down to a minimum of 1 shard."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	dryRun, err = strconv.ParseBool(os.Getenv("DRY_RUN"))
	if err != nil {
		// Default dryRun value is true. In case of error while reading this variable, dryRun will be set to true to be on the safer side.
		dryRun = true
		logMessage := "Error while reading the DRY_RUN environment variable. The default value of true will be used implying this will run as a dry run."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}
	var currentAlarmAction string
	var newShardCount, currentShardCount int64
	var alarmActions []*string

	// Retrieve the SNS message from the Lambda context.
	// Retrieve the alarm that triggered lambda from SNS message.
	// List the tags for the current alarm.
	// Example: If a scale-up alarm triggered us, its tags contain the name of its complimentary scale-down alarm, which we must also update.
	// Retrieve the complimentary alarm name.
	// Assign values to scaleUpAlarmName and scaleDownAlarmName variable.
	// Figure out whether the scale action is "Up" or "Down".
	snsRecord := snsEvent.Records[0].SNS
	message := snsRecord.Message
	alarmActions = append(alarmActions, &snsRecord.TopicArn)
	err = json.Unmarshal([]byte(message), &alarmInformation)
	if err != nil {
		logMessage := "Log json.Unmarshal error while parsing the SNS message."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
		return
	}
	var currentAlarmName = alarmInformation["AlarmName"].(string)
	logger = logger.WithField("CurrentAlarmName", currentAlarmName)
	response, err := svcCloudWatch.ListTagsForResource(&cloudwatch.ListTagsForResourceInput{
		ResourceARN: aws.String(alarmInformation["AlarmArn"].(string))})
	if err != nil {
		logMessage := "Log Cloudwatch ListTagsForResource API error"
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, true)
		return
	}
	scaleUpAlarmName, scaleDownAlarmName, currentAlarmAction, lastScaledTimestamp = parseAlarmNameAndTags(*response, currentAlarmName)
	logger = logger.WithField("ScaleAction", currentAlarmAction)
	if currentAlarmAction == "" {
		logMessage := fmt.Sprintf("Scaling event was rejected. Could not parse triggering alarm name (%s), should end in -scale-up or -scale-down", currentAlarmName)
		err = errors.New(logMessage)
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, true)
		return
	}

	// Retrieve the stream name from the SNS message.
	// List the tags for the stream.
	// Retry up to throttleRetryCount times if LimitExceededException occurs on the ListTagsForStream API call.
	// Example: SNS scaling event time must be newer than the stream's LastScaledTimestamp. This is a sanity check to guard against
	//          multiple scaling events coming in before a stream was able to scale or similar race conditions. We do not expect this
	//          in normal operation, it is just a sanity check so a stream can't scale out of control in some unforeseen scenario.
	// From the LastScaledTimestamp figure out whether this is a valid scaling attempt.
	// Ignore if not a valid scaling attempt.
	streamName := getStreamName(alarmInformation)
	logger = logger.WithField("StreamName", streamName)
	logger.Info("Received scaling event. Will now scale the stream")
	scaleStream := checkLastScaledTimestamp(lastScaledTimestamp, alarmInformation["StateChangeTime"].(string), 0)
	if !scaleStream {
		//Ignore this attempt and exit.
		logger.Info("Scale-" + currentAlarmAction + " event rejected")
		_, err = setAlarmState(currentAlarmName, cloudwatch.StateValueInsufficientData, "Scale-"+currentAlarmAction+" event rejected. Changing alarm state back to Insufficient Data.")
		if err != nil {
			logMessage := fmt.Sprintf("Scaling event was rejected but couldn't set the alarm (%s) state to OK.", currentAlarmName)
			logger.WithError(err).Error(logMessage)
			errorHandler(err, logMessage, currentAlarmName, false)
		}
		return
	}

	// Describe stream summary.
	// Grab the shard count from the stream summary.
	// Calculate the target shard count and downThreshold based on the current shard count and scaling action.
	// Check for dry run. If dry run, log and exit.
	streamSummary, err := svcKinesis.DescribeStreamSummary(&kinesis.DescribeStreamSummaryInput{
		StreamName: &streamName})
	if err != nil {
		logMessage := "Log Kinesis DescribeStreamSummary API error"
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, true)
		return
	}
	currentShardCount = *((*streamSummary.StreamDescriptionSummary).OpenShardCount)
	newShardCount, downThreshold = calculateNewShardCount(currentAlarmAction, downThreshold, currentShardCount, minShardCount)
	logger = logger.WithField("CurrentShardCount", currentShardCount).WithField("TargetShardCount", newShardCount)
	if dryRun {
		logger.Info("This is dry run. Will not scale the stream.")
		return
	}
	logger.Info("Target shard count calculated. Now scaling the stream.")

	// Update the stream with the new shard count.
	// Set the timestamp for successful stream scaling.
	_, err = svcKinesis.UpdateShardCount(&kinesis.UpdateShardCountInput{
		ScalingType:      aws.String(kinesis.ScalingTypeUniformScaling),
		StreamName:       &streamName,
		TargetShardCount: &newShardCount})
	if err != nil {
		logMessage := "Log Kinesis UpdateShardCount API error"
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, true)
		return
	}
	alarmLastScaledTimestampValue := (time.Now()).Format("2006-01-02T15:04:05.000+0000")

	// Update the scale up alarm.
	// Set the state of the scale up alarm to INSUFFICIENT_DATA.
	_, err = updateAlarm(scaleUpAlarmName, evaluationPeriodScaleUp, datapointsRequiredScaleUp, upThreshold, cloudwatch.ComparisonOperatorGreaterThanOrEqualToThreshold, streamName, alarmActions, newShardCount, false, 0)
	if err != nil {
		logMessage := fmt.Sprintf("Kinesis stream (%s) has scaled and been tagged with the timestamp but couldn't update the scale-up alarm (%s). Log CloudWatch PutMetricAlarm API error.", streamName, scaleUpAlarmName)
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, false)
	}
	_, err = setAlarmState(scaleUpAlarmName, cloudwatch.StateValueInsufficientData, "Metric math and threshold value update")
	if err != nil {
		logMessage := fmt.Sprintf("Kinesis stream (%s) has scaled and been tagged with the timestamp. Scale-up alarm(%s) has been updated but couldn't set the state to INSUFFICIENT_DATA. Log CloudWatch SetAlarmState API error.", streamName, scaleUpAlarmName)
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, false)
	}

	// Update the scale down alarm.
	// Set the state of the scale down alarm to INSUFFICIENT_DATA.
	_, err = updateAlarm(scaleDownAlarmName, evaluationPeriodScaleDown, datapointsRequiredScaleDown, downThreshold, cloudwatch.ComparisonOperatorLessThanThreshold, streamName, alarmActions, newShardCount, true, scaleDownMinIterAgeMins)
	if err != nil {
		logMessage := fmt.Sprintf("Kinesis stream (%s) has scaled and been tagged with the timestamp but couldn't update the scale-down alarm (%s). Log CloudWatch PutMetricAlarm API error.", streamName, scaleDownAlarmName)
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, false)
	}
	_, err = setAlarmState(scaleDownAlarmName, cloudwatch.StateValueInsufficientData, "Metric math and threshold value update")
	if err != nil {
		logMessage := fmt.Sprintf("Kinesis stream (%s) has scaled and been tagged with the timestamp. Scale-down alarm(%s) has been updated but couldn't set the state to INSUFFICIENT_DATA. Log CloudWatch SetAlarmState API error.", streamName, scaleDownAlarmName)
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, false)
	}

	// Get the ARNs for both the alarms.
	// Tag the scale-up alarm.
	// Tag the scale-down alarm.
	scaleUpAlarmArn, scaleDownAlarmArn, err := getAlarmArn(scaleUpAlarmName, scaleDownAlarmName)
	if err != nil {
		logMessage := fmt.Sprintf("Kinesis stream (%s) has been scaled. Scale-up alarm(%s) and scale-down alarm(%s) have been updated and set to INSUFFICIENT_DATA. Failed while getting the ARNs for the alarms. Won't be possible to tag the alarms. Log CloudWatch DescribeAlarms API error", streamName, scaleUpAlarmName, scaleDownAlarmName)
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, false)
		return
	}
	_, err = tagAlarm(scaleUpAlarmArn, "ScaleAction", "ComplimentaryAlarm", "Up", scaleDownAlarmName, alarmLastScaledTimestampValue)
	if err != nil {
		logMessage := fmt.Sprintf("Kinesis stream (%s) has been scaled. Scale-up alarm(%s) and scale-down alarm(%s) have been updated and set to INSUFFICIENT_DATA. Failed while tagging the scale-up alarm with ScaleAction and ComplimentaryAlarm tags. Log CloudWatch TagResource API error", streamName, scaleUpAlarmName, scaleDownAlarmName)
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, false)
	}
	_, err = tagAlarm(scaleDownAlarmArn, "ScaleAction", "ComplimentaryAlarm", "Down", scaleUpAlarmName, alarmLastScaledTimestampValue)
	if err != nil {
		logMessage := fmt.Sprintf("Kinesis stream (%s) has been scaled. Scale-up alarm(%s) and scale-down alarm(%s) have been updated and set to INSUFFICIENT_DATA. Failed while tagging the scale-down alarm with ScaleAction and ComplimentaryAlarm tags. Log CloudWatch TagResource API error", streamName, scaleUpAlarmName, scaleDownAlarmName)
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, currentAlarmName, false)
	}

	// Update the concurrency on the processing lambda function.
	updateProcessingLambdaConcurrency(newShardCount)

	logger.Info(fmt.Sprintf("Scaling complete for %s", streamName))
	// Successful exit, shard count has been updated, concurrency for the processing lambda has been set accordingly, alarms have been re-calculated and updated.
}

// UpdateAlarm updates the cloudwatch alarm with an updated shardCount values (parameter newShardCount).
// All other metric math definitions remain the same.
// It returns the output and error from the CloudWatch PutMetricAlarm API.
// Parameters:
// alarmName: Name of the alarm
// evaluationPeriod: Period after which the data for the alarm will be evaluated
// datapointsRequired: Number of datapoints required in the evaluationPeriod to trigger the alarm
// threshold: The threshold at which the alarm will trigger the actions
// comparisonOperator: Operator to compare with the threshold
// streamName: Name of the stream for which the alarm is being updated
// alarmActions: The list of SNS topics the alarm should send message to when in ALARM state
// newShardCount: The new shard count of the Kinesis Data Stream
// isScaledown: true if the alarm is for scale down, false if for scale up
// scaleDownMinIterAgeMins: used for scaleDown only metrics
func updateAlarm(alarmName string, evaluationPeriod int64, datapointsRequired int64, threshold float64, comparisonOperator string, streamName string, alarmActions []*string, newShardCount int64, isScaleDown bool, scaleDownMinIterAgeMins int64) (*cloudwatch.PutMetricAlarmOutput, error) {
	var putMetricAlarmResponse *cloudwatch.PutMetricAlarmOutput
	var err error
	// Initialize the seed function to get a different random number every execution.
	rand.Seed(time.Now().UnixNano())
	var periodMins int64 = 5 // Data is evaluated every 5 minutes
	var retryCount int64 = 0
	var isDone bool

	var metrics []*cloudwatch.MetricDataQuery

	// Add m1
	metrics = append(metrics, &cloudwatch.MetricDataQuery{
		Id:         aws.String("m1"),
		Label:      aws.String(kinesis.MetricsNameIncomingBytes),
		ReturnData: aws.Bool(false),
		MetricStat: &cloudwatch.MetricStat{
			Metric: &cloudwatch.Metric{
				Namespace:  aws.String("AWS/Kinesis"),
				MetricName: aws.String(kinesis.MetricsNameIncomingBytes),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("StreamName"),
						Value: aws.String(streamName),
					},
				},
			},
			Period: aws.Int64(60 * periodMins),
			Stat:   aws.String(cloudwatch.StatisticSum),
		},
	})
	// Add m2
	metrics = append(metrics, &cloudwatch.MetricDataQuery{
		Id:         aws.String("m2"),
		Label:      aws.String(kinesis.MetricsNameIncomingRecords),
		ReturnData: aws.Bool(false),
		MetricStat: &cloudwatch.MetricStat{
			Metric: &cloudwatch.Metric{
				Namespace:  aws.String("AWS/Kinesis"),
				MetricName: aws.String(kinesis.MetricsNameIncomingRecords),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("StreamName"),
						Value: aws.String(streamName),
					},
				},
			},
			Period: aws.Int64(60 * periodMins),
			Stat:   aws.String(cloudwatch.StatisticSum),
		},
	})
	// Add m3 (if scale down)
	if isScaleDown {
		metrics = append(metrics, &cloudwatch.MetricDataQuery{
			Id:         aws.String("m3"),
			Label:      aws.String("GetRecords.IteratorAgeMilliseconds"),
			ReturnData: aws.Bool(false),
			MetricStat: &cloudwatch.MetricStat{
				Metric: &cloudwatch.Metric{
					Namespace:  aws.String("AWS/Kinesis"),
					MetricName: aws.String("GetRecords.IteratorAgeMilliseconds"),
					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("StreamName"),
							Value: aws.String(streamName),
						},
					},
				},
				Period: aws.Int64(60 * periodMins),
				Stat:   aws.String(cloudwatch.StatisticMaximum),
			},
		})
	}
	// Add e1, e2, e3, e4
	metrics = append(metrics, &cloudwatch.MetricDataQuery{
		Id:         aws.String("e1"),
		Expression: aws.String("FILL(m1,0)"),
		Label:      aws.String("FillMissingDataPointsWithZeroForIncomingBytes"),
		ReturnData: aws.Bool(false),
	})
	metrics = append(metrics, &cloudwatch.MetricDataQuery{
		Id:         aws.String("e2"),
		Expression: aws.String("FILL(m2,0)"),
		Label:      aws.String("FillMissingDataPointsWithZeroForIncomingRecords"),
		ReturnData: aws.Bool(false),
	})
	metrics = append(metrics, &cloudwatch.MetricDataQuery{
		Id:         aws.String("e3"),
		Expression: aws.String(fmt.Sprintf("e1/(1024*1024*60*%d*s1)", periodMins)),
		Label:      aws.String("IncomingBytesUsageFactor"),
		ReturnData: aws.Bool(false),
	})
	metrics = append(metrics, &cloudwatch.MetricDataQuery{
		Id:         aws.String("e4"),
		Expression: aws.String(fmt.Sprintf("e2/(1000*60*%d*s1)", periodMins)),
		Label:      aws.String("IncomingRecordsUsageFactor"),
		ReturnData: aws.Bool(false),
	})
	// Add e5 (if scale down)
	if isScaleDown {
		metrics = append(metrics, &cloudwatch.MetricDataQuery{
			Id:         aws.String("e5"),
			Expression: aws.String(fmt.Sprintf("(FILL(m3,0)/1000/60)*(%0.5f/s2)", threshold)),
			Label:      aws.String("IteratorAgeAdjustedFactor"),
			ReturnData: aws.Bool(false),
		})
	}
	// Add e6
	if isScaleDown {
		metrics = append(metrics, &cloudwatch.MetricDataQuery{
			Id:         aws.String("e6"),
			Expression: aws.String("MAX([e3,e4,e5])"), // Scale down takes into account e5 (max iterator age), add it here
			Label:      aws.String("MaxIncomingUsageFactor"),
			ReturnData: aws.Bool(true),
		})
	} else {
		metrics = append(metrics, &cloudwatch.MetricDataQuery{
			Id:         aws.String("e6"),
			Expression: aws.String("MAX([e3,e4])"), // Scale up doesn't look at iterator age, only bytes/sec, records/sec
			Label:      aws.String("MaxIncomingUsageFactor"),
			ReturnData: aws.Bool(true),
		})
	}
	// Add s1
	metrics = append(metrics, &cloudwatch.MetricDataQuery{
		Id:         aws.String("s1"),
		Expression: aws.String(fmt.Sprintf("%d", newShardCount)),
		Label:      aws.String("ShardCount"),
		ReturnData: aws.Bool(false),
	})
	// Add s2
	if isScaleDown {
		metrics = append(metrics, &cloudwatch.MetricDataQuery{
			Id:         aws.String("s2"),
			Expression: aws.String(fmt.Sprintf("%d", scaleDownMinIterAgeMins)),
			Label:      aws.String("IteratorAgeMinutesToBlockScaleDowns"),
			ReturnData: aws.Bool(false),
		})
	}

	for retryCount < throttleRetryCount {
		putMetricAlarmResponse, err = svcCloudWatch.PutMetricAlarm(&cloudwatch.PutMetricAlarmInput{
			AlarmName:          aws.String(alarmName),
			AlarmDescription:   aws.String("Alarm to scale Kinesis stream"),
			ActionsEnabled:     aws.Bool(true),
			AlarmActions:       alarmActions,
			EvaluationPeriods:  &evaluationPeriod,
			DatapointsToAlarm:  &datapointsRequired,
			Threshold:          &threshold,
			ComparisonOperator: aws.String(comparisonOperator),
			TreatMissingData:   aws.String("ignore"),
			Metrics:            metrics,
		})
		if err == nil {
			break
		} else {
			errorCode, _ := getAwsErrFromError(err)
			switch errorCode {
			case cloudwatch.ErrCodeLimitExceededFault:
				// The limit on the PutMetricAlarm has been hit. Sleep and retry up to ten times.
				retryCount++
				sleepTime := rand.Int63n(throttleRetryMax-throttleRetryMin+1) + throttleRetryMin
				time.Sleep(time.Duration(sleepTime) * time.Second)
			default:
				// Error other than LimitExceeded, report the error
				isDone = true
			}
		}
		if isDone {
			break
		}
	}
	return putMetricAlarmResponse, err
}

// SetAlarmState updates the state of the alarm.
// The state is set to OK in case of any unexpected errors (except Kinesis ResourceInUseException error).
// The state is set to INSUFFICIENT_DATA after the stream scales successfully.
// It returns the response and error from the CloudWatch SetAlarmState API.
// Parameters:
// alarmName: Name of the alarm
// state: Target state of the alarm
// reason: String for the reason of the state transition
func setAlarmState(alarmName string, state string, reason string) (*cloudwatch.SetAlarmStateOutput, error) {
	var setAlarmStateResponse *cloudwatch.SetAlarmStateOutput
	var err error
	// Initialize the seed function to get a different random number every execution.
	rand.Seed(time.Now().UnixNano())
	var retryCount int64 = 0
	var isDone bool
	for retryCount < throttleRetryCount {
		setAlarmStateResponse, err = svcCloudWatch.SetAlarmState(&cloudwatch.SetAlarmStateInput{
			AlarmName:   aws.String(alarmName),
			StateValue:  &state,
			StateReason: &reason,
		})
		if err == nil {
			break
		} else {
			errorCode, _ := getAwsErrFromError(err)
			switch errorCode {
			case cloudwatch.ErrCodeLimitExceededFault:
				// The limit on the PutMetricAlarm has been hit. Sleep and retry up to ten times.
				retryCount++
				sleepTime := rand.Int63n(throttleRetryMax-throttleRetryMin+1) + throttleRetryMin
				time.Sleep(time.Duration(sleepTime) * time.Second)
			default:
				// Error other than LimitExceeded, report the error
				isDone = true
			}
		}
		if isDone {
			break
		}
	}

	return setAlarmStateResponse, err
}

// CalculateNewShardCount computes the new shard count based on the current shards and the scaling action
// It returns the new shard count and the scaling down threshold
// Parameters:
// scaleAction: The scaling action. Possible values are Up and Down
// downThreshold: The current scaling down threshold. This will be set to -1.0 if the new shard count turns out to be 1
// currentShardCount: The current open shards in the Kinesis stream
func calculateNewShardCount(scaleAction string, downThreshold float64, currentShardCount int64, minShardCount int64) (int64, float64) {
	var targetShardCount int64
	if scaleAction == "Up" {
		targetShardCount = currentShardCount * 2
	}

	if scaleAction == "Down" {
		targetShardCount = currentShardCount / 2
		// Set to minimum shard count
		if targetShardCount <= minShardCount {
			targetShardCount = minShardCount
			// At minimum shard count,set the scale down threshold to -1, so that scale down alarm remains in OK state
			downThreshold = -1.0
		}
	}
	return targetShardCount, downThreshold
}

// GetStreamName retrieves the stream name from the SNS Message.
// Structure of the SNS Message can be seen here: https://github.com/aws/aws-lambda-go/blob/master/events/sns.go
// SNS JSON that this function parses to retrieve the stream name: {"AlarmName": "Kinesis_Auto_Scale_Up_Alarm","AlarmDescription": "Alarm to scale up Kinesis stream","AWSAccountId": "111111111111","NewStateValue": "ALARM","NewStateReason": "Threshold Crossed: 1 out of the last 1 datapoints [0.43262672424316406 (23/04/20 21:16:00)] was greater than or equal to the threshold (0.4) (minimum 1 datapoint for OK -> ALARM transition).","StateChangeTime": "2020-04-23T21:17:44.775+0000","Region": "US East (Ohio)","AlarmArn": "arn:aws:cloudwatch:us-east-2:111111111111:alarm:Kinesis_Auto_Scale_Up_Alarm","OldStateValue": "OK","Trigger": {"Period": 60,"EvaluationPeriods": 1,"ComparisonOperator": "GreaterThanOrEqualToThreshold","Threshold": 0.4,"TreatMissingData": "- TreatMissingData:                    ignore","EvaluateLowSampleCountPercentile": "","Metrics": [{"Id": "m1","Label": "IncomingBytes","MetricStat": {"Metric": {"Dimensions": [{"value": "auto-scaling-demo-stream","name": "StreamName"}],"MetricName": "IncomingBytes","Namespace": "AWS/Kinesis"},"Period": 60,"Stat": "Sum"},"ReturnData": false},{"Id": "m2","Label": "IncomingRecords","MetricStat": {"Metric": {"Dimensions": [{"value": "auto-scaling-demo-stream","name": "StreamName"}],"MetricName": "IncomingRecords","Namespace": "AWS/Kinesis"},"Period": 60,"Stat": "Sum"},"ReturnData": false},{"Expression": "FILL(m1,0)","Id": "e1","Label": "FillMissingDataPointsWithZeroForIncomingBytes","ReturnData": false},{"Expression": "FILL(m2,0)","Id": "e2","Label": "FillMissingDataPointsWithZeroForIncomingRecords","ReturnData": false},{"Expression": "e1/(1024*1024*60*1)","Id": "e3","Label": "IncomingBytesUsageFactor","ReturnData": false},{"Expression": "e2/(1000*60*1)","Id": "e4","Label": "IncomingRecordsUsageFactor","ReturnData": false},{"Expression": "MAX([e3,e4])","Id": "e5","Label": "ScalingTrigger","ReturnData": true}]}
// It returns the stream name
// Parameters:
// alarmInformation: The message property from the SNSEntity object.
func getStreamName(alarmInformation aws.JSONValue) string {
	var stream string
	for _, metric := range alarmInformation["Trigger"].(map[string]interface{})["Metrics"].([]interface{}) {
		if metric.(map[string]interface{})["MetricStat"] != nil {
			if metric.(map[string]interface{})["Id"] == "m1" || metric.(map[string]interface{})["Id"] == "m2" {
				for _, dimension := range metric.(map[string]interface{})["MetricStat"].(map[string]interface{})["Metric"].(map[string]interface{})["Dimensions"].([]interface{}) {
					stream = dimension.(map[string]interface{})["value"].(string)
				}
				break
			}
		}
	}
	return stream
}

// parseAlarmNameAndTags figures out the scale action and the names for the scale-up and scale-down alarms based on the alarm name that triggered this lambda
// It returns both the alarm names and the scaling action of the current alarm
// Parameters:
// listTags: The tag list response from the CloudWatch ListTagsForResource API
// currentAlarmName: The name of the alarm that triggered the invocation
func parseAlarmNameAndTags(listTags cloudwatch.ListTagsForResourceOutput, currentAlarmName string) (string, string, string, string) {
	var scaleUpAlarmName, scaleDownAlarmName, currentAlarmAction, lastScaledTimestamp string
	var scaleDownSuffix = "-scale-down"
	var scaleUpSuffix = "-scale-up"

	if strings.HasSuffix(currentAlarmName, scaleUpSuffix) {
		currentAlarmAction = "Up"
		scaleUpAlarmName = currentAlarmName
		scaleDownAlarmName = currentAlarmName[0:len(currentAlarmName)-len(scaleUpSuffix)] + scaleDownSuffix
	} else if strings.HasSuffix(currentAlarmName, scaleDownSuffix) {
		currentAlarmAction = "Down"
		scaleUpAlarmName = currentAlarmName[0:len(currentAlarmName)-len(scaleDownSuffix)] + scaleUpSuffix
		scaleDownAlarmName = currentAlarmName
	} else {
		// Error unhandled
	}

	for _, tag := range listTags.Tags {
		if aws.StringValue(tag.Key) == "LastScaledTimestamp" {
			lastScaledTimestamp = aws.StringValue(tag.Value)
		}
	}

	return scaleUpAlarmName, scaleDownAlarmName, currentAlarmAction, lastScaledTimestamp
}

// TagAlarm tags the alarms with two tags, keys being ScaleAction and ComplimentaryAlarm.
// It returns the response and error for the CloudWatch TagResource API
// Parameters:
// alarmArn: ARN of the alarm
// scaleAction: Tag key. Valid value: ScaleAction
// complimentaryAlarm: Tag key. Valid value: ComplimentaryAlarm
// actionValue: Scale action that the alarm is used for. Valid values: Up or Down
// alarmName: The name of the alarm that is complimentary to the alarm being tagged. Valid value: Name of the alarm
func tagAlarm(alarmArn string, scaleAction string, complimentaryAlarm string, actionValue string, alarmValue string, lastScaledTimestamp string) (*cloudwatch.TagResourceOutput, error) {
	// Tag the scale down alarm
	tagAlarmResponse, err := svcCloudWatch.TagResource(&cloudwatch.TagResourceInput{
		ResourceARN: &alarmArn,
		Tags: []*cloudwatch.Tag{
			{
				Key:   &scaleAction,
				Value: &actionValue,
			},
			{
				Key:   &complimentaryAlarm,
				Value: &alarmValue,
			},
			{
				Key:   aws.String("LastScaledTimestamp"),
				Value: &lastScaledTimestamp,
			},
		},
	})
	return tagAlarmResponse, err
}

// CheckLastScaledTimestamp checks the timestamp tag on the Kinesis Stream to gauge if this is a valid scaling event
// It returns a boolean indicating whether the stream should be scaled at all
// Parameters:
// tagList: List of tags on the stream. Response from ListTagsForStream Kinesis API
// alarmTime: The time at which the alarm transitioned into ALARM state
// scalingPeriodMins: How often scaling may occur in this direction (up or down) in minutes
func checkLastScaledTimestamp(lastScaledTimestamp string, alarmTime string, scalingPeriodMins int64) bool {
	var scaleStream bool
	var firstEverScaleAttempt = true

	if lastScaledTimestamp == "" {
		firstEverScaleAttempt = true
	} else {
		firstEverScaleAttempt = false
	}

	// First attempt, scale the stream without checking the timestamp.
	if firstEverScaleAttempt {
		scaleStream = true
		return scaleStream
	}

	var stateChangeTime, stateChangeParseErr = time.Parse("2006-01-02T15:04:05.000+0000", alarmTime)
	var lastScaled, lastScaledTimestampParseErr = time.Parse("2006-01-02T15:04:05.000+0000", lastScaledTimestamp)

	// Time format invalid. Ignore timestamp check and scale the stream.
	if lastScaledTimestampParseErr != nil || stateChangeParseErr != nil {
		scaleStream = true
		return scaleStream
	}

	// This is an old alarm. Stream has already been scaled after this alarm was raised.
	if stateChangeTime.Before(lastScaled) || stateChangeTime.Equal(lastScaled) {
		scaleStream = false
		return scaleStream
	}

	// Too soon since the last scaling event, do not scale (unused feature right now and set to 0)
	var nextAllowedScalingEvent = lastScaled.Add(time.Minute * time.Duration(scalingPeriodMins))
	if stateChangeTime.Before(nextAllowedScalingEvent) {
		scaleStream = false
		return scaleStream
	}

	scaleStream = true
	return scaleStream
}

// ErrorHandler is a generic error handler.
// The function logs the error and sends a message to the pagerduty sns topic (except for ResourceInUseException Kinesis API).
// The alarm state is set to OK for the alarm to retry the scaling attempt
// Parameters:
// err: Error returned from the APIs
// message: The message giving more information about the error. Message will be logged and also published to sns topic
// currentAlarmName: Alarm name for which state has to set to OK explicitly
func errorHandler(err error, message string, currentAlarmName string, alarmStateChange bool) {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case kinesis.ErrCodeResourceInUseException:
			_, setAlarmStateError := setAlarmState(currentAlarmName, cloudwatch.StateValueOk, "ResourceInUse Exception from Kinesis, alarm state changed for alarm to retry scaling")
			logger.WithError(setAlarmStateError).Error("setAlarmState API error.")
		default:
			if alarmStateChange {
				_, setAlarmStateError := setAlarmState(currentAlarmName, cloudwatch.StateValueOk, message)
				logger.WithError(setAlarmStateError).Error("setAlarmState API error.")
			}
			emitScalingLambdaMetrics(fatalErrorMetric)
		}
	} else {
		// Any other error than AWS API error.
		emitScalingLambdaMetrics(fatalErrorMetric)
	}
}

// GetAlarmArn retrieves the arn of the alarm given the alarm name.
// It returns the scale-up alarm arn, scale-down alarm arn and the error from the CloudWatch DescribeAlarms API
// Parameters:
// scaleUpAlarmName: Alarm name
// scaleDownAlarmName: Alarm name
func getAlarmArn(scaleUpAlarmName string, scaleDownAlarmName string) (string, string, error) {
	var scaleUpAlarmArn, scaleDownAlarmArn string
	// Get the arn for the alarms
	describeAlarmsResponse, err := svcCloudWatch.DescribeAlarms(&cloudwatch.DescribeAlarmsInput{
		AlarmNames: []*string{&scaleUpAlarmName, &scaleDownAlarmName},
	})
	if err != nil {
		return scaleUpAlarmName, scaleDownAlarmName, err
	}
	for _, alarm := range describeAlarmsResponse.MetricAlarms {
		if *alarm.AlarmName == scaleUpAlarmName {
			scaleUpAlarmArn = *alarm.AlarmArn
		}
		if *alarm.AlarmName == scaleDownAlarmName {
			scaleDownAlarmArn = *alarm.AlarmArn
		}
	}
	return scaleUpAlarmArn, scaleDownAlarmArn, err
}

// GetAwsErrFromError retrieves error code and error message from the error object returned by the AWS APIs.
// It returns the error code and error message.
// Parameters:
// err: Error object returned by AWS API
func getAwsErrFromError(err error) (string, string) {
	var errorCode, errorMessage string
	if awsErr, ok := err.(awserr.Error); ok {
		errorCode = awsErr.Code()
		errorMessage = awsErr.Message()
	}
	return errorCode, errorMessage
}

// UpdateProducerConcurrency updates the reserved function concurrency for the cloudwatch producer lambda function that consumes data from Kinesis Data Streams.
// It returns the lambda PutFunctionConcurrency API response and error(if any).
// Parameters:
// newShardCount: The number of shards Kinesis Data Stream has been scaled to. Producer function concurrency should be equal to number of shards.
// producerFunctionArn: The arn of the producer function for which the reserved concurrency has to be updated.
func updateConcurrency(newShardCount int64, producerFunctionArn string) (*lambdaService.PutFunctionConcurrencyOutput, error) {
	response, err := lambdaClient.PutFunctionConcurrency(&lambdaService.PutFunctionConcurrencyInput{
		FunctionName:                 aws.String(producerFunctionArn),
		ReservedConcurrentExecutions: aws.Int64(newShardCount),
	})
	return response, err
}

// DeleteProducerConcurrency updates the function to use Unreserved concurrency in the account.
// This function will only be called if UpdateProducerConcurrency fails for some reason.
// It returns the lambda DeleteFunctionConcurrency API response and error (if any).
// Parameters:
// producerFunctionArn: The arn of the producer function which should be set to use the unreserved function concurrency.
func deleteConcurrency(producerFunctionArn string) (*lambdaService.DeleteFunctionConcurrencyOutput, error) {
	response, err := lambdaClient.DeleteFunctionConcurrency(&lambdaService.DeleteFunctionConcurrencyInput{
		FunctionName: &producerFunctionArn,
	})
	return response, err
}

// UpdateProcessingLambdaConcurrency updates the processing function concurrency.
// This function will set the function concurrency to the product of the processing lambdas per shard and new shard count.
// If setting the concurrency fails, the processing function will be set to use the unreserved account concurrency.
// Parameters:
// newShardCount: The new shard count that the stream is being scaled to.
func updateProcessingLambdaConcurrency(newShardCount int64) {
	var processingLambdaArn = os.Getenv("PROCESSING_LAMBDA_ARN")
	if processingLambdaArn == "" {
		// Empty value for processing lambda arn implies not expected to update the concurrency for any lambda function. Exiting.
		return
	}

	processingLambdasPerShard, err := strconv.ParseInt(os.Getenv("PROCESSING_LAMBDAS_PER_SHARD"), 10, 64)
	if err != nil {
		// Default processing lambdas per shard.
		processingLambdasPerShard = 1 // Set to 1 processing lambda function per shard
		logMessage := "Error reading the PROCESSING_LAMBDAS_PER_SHARD environment variable. Concurrency for the processing function will be set with a default value of 1 processing lambda function per shard."
		logger.WithError(err).Error(logMessage)
		errorHandler(err, logMessage, "", false)
	}

	// Total concurrency required for the function will be a product of the shard count on the stream and number of lambdas consuming per stream.
	totalConcurrency := newShardCount * processingLambdasPerShard
	_, err = updateConcurrency(totalConcurrency, processingLambdaArn)
	if err != nil {
		logMessage := fmt.Sprintf("Kinesis stream has been scaled. Scale-up alarm and scale-down alarm have been updated and set to INSUFFICIENT_DATA. Failed while updating producer function(%s) concurrency. Will set the function to Unreserved concurrency. Log Lambda PutFunctionConcurrency API error", processingLambdaArn)
		logger.WithError(err).Error(logMessage)

		// Failed to set reserved concurrency. Will now use unreserved concurrency in the account.
		_, err = deleteConcurrency(processingLambdaArn)
		if err != nil {
			logMessage := fmt.Sprintf("Failed to set the function (%s) to use unreserved concurrency.", processingLambdaArn)
			logger.WithError(err).Error(logMessage)
			errorHandler(err, logMessage, "", false)
		}
	}
}

// EmitScalingLambdaMetrics updates a custom metric implying fatal error in the scaling lambda function.
// Parameters:
// metricName: The name of the metric that will be updated with increased value(+1) of count.
func emitScalingLambdaMetrics(metricName string) {
	_, err := svcCloudWatch.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("AWS/Lambda"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String(metricName),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(1.0),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("FunctionName"),
						Value: aws.String(functionName),
					},
				},
			},
		},
	})
	if err != nil {
		logger.WithError(err).Errorf("Error adding metrics: %v", err.Error())
		return
	}
}

func main() {
	// Create session
	var sess = session.Must(session.NewSession())

	// Create new Kinesis client
	svcKinesis = kinesis.New(sess)
	svcCloudWatch = cloudwatch.New(sess)
	lambdaClient = lambdaService.New(sess)

	lambda.Start(handleRequest)
}
