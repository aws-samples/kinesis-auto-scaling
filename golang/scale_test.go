// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	mockaws "./mocks/aws"
)

func init() {
	logs := logrus.New()
	logs.SetFormatter(&logrus.JSONFormatter{})
	logs.SetReportCaller(true)
	logs.SetLevel(logrus.InfoLevel)
	throttleRetryMin = 2
	throttleRetryMax = 5
	throttleRetryCount = 20
}

func TestGetStreamName(t *testing.T) {
	tests := []struct {
		name                 string
		snsAlarmDataFilePath string
		expectedStreamName   string
	}{
		{
			name:                 "SNS Scale Up Alarm",
			snsAlarmDataFilePath: "./TestAlarmJsons/Alarm1.json",
			expectedStreamName:   "auto-scaling-demo-stream",
		},
		{
			name:                 "SNS Scale Down Alarm",
			snsAlarmDataFilePath: "./TestAlarmJsons/Alarm2.json",
			expectedStreamName:   "auto-stream-poc",
		},
	}

	// Loop through the tests
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Read from the file.
			var alarm aws.JSONValue
			jsonFile, fileOpenErr := os.Open(tc.snsAlarmDataFilePath)
			byteValue, fileReadErr := ioutil.ReadAll(jsonFile)
			unmarshalErr := json.Unmarshal(byteValue, &alarm)
			require.Nil(t, fileOpenErr, "Error not nil while opening the file")
			require.Nil(t, fileReadErr, "Error not nil while reading file.")
			require.Nil(t, unmarshalErr, "Error not nil while un-marshaling the valid json string into aws.JSONValue")

			// Once read, test the function.
			streamName := getStreamName(alarm)
			require.Equal(t, tc.expectedStreamName, streamName,
				"Stream name not correct. Expected %v. Actual %v", tc.expectedStreamName, streamName)
		})
	}
}

func TestCalculateNewShardCount(t *testing.T) {
	tests := []struct {
		name                  string
		scaleAction           string
		downThreshold         float64
		currentShardCount     int64
		newShardCount         int64
		expectedDownThreshold float64
	}{
		{
			name:                  "Scale Up Action 1",
			scaleAction:           "Up",
			downThreshold:         0.4,
			currentShardCount:     16,
			newShardCount:         32,
			expectedDownThreshold: 0.4,
		},
		{
			name:                  "Scale Up Action 2",
			scaleAction:           "Up",
			downThreshold:         0.4,
			currentShardCount:     1,
			newShardCount:         2,
			expectedDownThreshold: 0.4,
		},
		{
			name:                  "Scale Down Action 1",
			scaleAction:           "Down",
			downThreshold:         0.4,
			currentShardCount:     32,
			newShardCount:         16,
			expectedDownThreshold: 0.4,
		},
		{
			name:                  "Scale Down Action 2",
			scaleAction:           "Down",
			downThreshold:         0.4,
			currentShardCount:     2,
			newShardCount:         1,
			expectedDownThreshold: -1.0,
		},
		{
			name:                  "Scale Down Action 3",
			scaleAction:           "Down",
			downThreshold:         0.4,
			currentShardCount:     1,
			newShardCount:         1,
			expectedDownThreshold: -1.0,
		},
	}

	// Loop through the tests
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var newShardCount int64
			var downThreshold float64
			newShardCount, downThreshold = calculateNewShardCount(tc.scaleAction, tc.downThreshold, tc.currentShardCount)
			require.Equal(t, tc.newShardCount, newShardCount,
				"New shard count is wrong. Expected new shard count: %d, Actual new shard count:%d", tc.newShardCount, newShardCount)
			require.Equal(t, tc.expectedDownThreshold, downThreshold,
				"Down threshold is wrong. Expected down threshold: %d, Actual down threshold:%d", tc.expectedDownThreshold, downThreshold)
		})
	}
}

func TestAlarmNameFromTags(t *testing.T) {
	var ActionKey = "ScaleAction"
	var scaleUpAlarm1ActionValue = "Up"
	var scaleDownAlarm1ActionValue = "Down"
	var CompAlarmKey = "ComplimentaryAlarm"
	var scaleUpAlarm1CompAlarmValue = "scale_down_alarm"
	var scaleDownAlarm1CompAlarmValue = "scale_up_alarm"
	var timestamp = (time.Now()).Format("2006-01-02T15:04:05.000+0000")
	tests := []struct {
		name                        string
		tagList                     cloudwatch.ListTagsForResourceOutput
		currentAlarmName            string
		expectedScaleUpAlarmName    string
		expectedScaleDownAlarmName  string
		expectedLastScaledTimestamp string
		expectedCurrentAlarmAction  string
	}{
		{
			name: "Scale Up Alarm 1",
			tagList: cloudwatch.ListTagsForResourceOutput{
				Tags: []*cloudwatch.Tag{
					{
						Key:   &ActionKey,
						Value: &scaleUpAlarm1ActionValue,
					},
					{
						Key:   &CompAlarmKey,
						Value: &scaleUpAlarm1CompAlarmValue,
					},
					{
						Key:   aws.String("LastScaledTimestamp"),
						Value: aws.String(timestamp),
					},
				},
			},
			currentAlarmName:            "scale_up_alarm",
			expectedScaleUpAlarmName:    "scale_up_alarm",
			expectedScaleDownAlarmName:  "scale_down_alarm",
			expectedLastScaledTimestamp: timestamp,
			expectedCurrentAlarmAction:  "Up",
		},
		{
			name: "Scale Down Alarm 1",
			tagList: cloudwatch.ListTagsForResourceOutput{
				Tags: []*cloudwatch.Tag{
					{
						Key:   &ActionKey,
						Value: &scaleDownAlarm1ActionValue,
					},
					{
						Key:   &CompAlarmKey,
						Value: &scaleDownAlarm1CompAlarmValue,
					},
					{
						Key:   aws.String("LastScaledTimestamp"),
						Value: aws.String(timestamp),
					},
				},
			},
			currentAlarmName:            "scale_down_alarm",
			expectedScaleUpAlarmName:    "scale_up_alarm",
			expectedScaleDownAlarmName:  "scale_down_alarm",
			expectedLastScaledTimestamp: timestamp,
			expectedCurrentAlarmAction:  "Down",
		},
	}

	// Loop through the tests
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scaleUpAlarmName, scaleDownAlarmName, currentAlarmAction, lastScaledTimestamp := alarmNameFromTags(tc.tagList, tc.currentAlarmName)
			require.Equal(t, tc.expectedScaleUpAlarmName, scaleUpAlarmName,
				"Scale up alarm name is wrong. Expected scale up alarm name: %v, Actual:%v", tc.expectedScaleUpAlarmName, scaleUpAlarmName)
			require.Equal(t, tc.expectedScaleDownAlarmName, scaleDownAlarmName,
				"Scale down alarm name is wrong. Expected scale up alarm name: %v, Actual:%v", tc.expectedScaleDownAlarmName, scaleDownAlarmName)
			require.Equal(t, tc.expectedCurrentAlarmAction, currentAlarmAction,
				"Current alarm action is wrong. Expected current alarm action: %v, Actual:%v", tc.expectedCurrentAlarmAction, currentAlarmAction)
			require.Equal(t, tc.expectedLastScaledTimestamp, lastScaledTimestamp,
				" Last scaled timestamp is wrong. Expected: %v, Actual:%v", tc.expectedLastScaledTimestamp, lastScaledTimestamp)
		})
	}
}

func TestCheckLastScaledTimestamp(t *testing.T) {
	var lastScaledTime = "2020-05-21T01:25:38.326+0000"
	var invalidTimestamp1 = ""
	var invalidTimestamp2 = "abc"
	tests := []struct {
		name                       string
		lastScaledTimestamp        string
		alarmTime                  string
		evaluationPeriod           int64
		expectedScaleStreamBoolean bool
	}{
		// Scale ups
		{
			name:                       "Valid scale-up attempt (scaling period)",
			lastScaledTimestamp:        lastScaledTime,
			alarmTime:                  "2020-05-21T01:46:38.326+0000", // at or after LastScaledTimestamp + evaluationPeriod(Mins) = valid
			evaluationPeriod:           0,
			expectedScaleStreamBoolean: true,
		},
		{
			name:                       "Invalid scale-up attempt (scaling period)",
			lastScaledTimestamp:        lastScaledTime,
			alarmTime:                  "2020-05-21T01:44:38.326+0000", // before LastScaledTimestamp + evaluationPeriod(Mins) = invalid
			evaluationPeriod:           20,
			expectedScaleStreamBoolean: false,
		},
		// Scale downs
		{
			name:                       "Valid scale-down attempt (scaling period)",
			lastScaledTimestamp:        lastScaledTime,
			alarmTime:                  "2020-05-21T07:26:38.326+0000", // at or after LastScaledTimestamp + evaluationPeriod(Mins) = valid
			evaluationPeriod:           0,
			expectedScaleStreamBoolean: true,
		},
		{
			name:                       "Invalid scale-down attempt (scaling period)",
			lastScaledTimestamp:        lastScaledTime,
			alarmTime:                  "2020-05-21T07:24:38.326+0000", // before LastScaledTimestamp + evaluationPeriod(Mins) = invalid
			evaluationPeriod:           360,
			expectedScaleStreamBoolean: false,
		},
		// Race condition edge cases
		{
			name:                       "Invalid scaling attempt (race condition equal)",
			lastScaledTimestamp:        lastScaledTime,
			alarmTime:                  "2020-05-21T01:25:38.326+0000", /* at or before LastScaledTimestamp = invalid */
			evaluationPeriod:           0,
			expectedScaleStreamBoolean: false,
		},
		{
			name:                       "Invalid scaling attempt (race condition before)",
			lastScaledTimestamp:        lastScaledTime,
			alarmTime:                  "2020-05-21T01:24:38.326+0000", /* at or before LastScaledTimestamp = invalid */
			evaluationPeriod:           0,
			expectedScaleStreamBoolean: false,
		},
		// Fist / uninitialized / bad data cases
		{
			name:                       "First ever scaling attempt",
			lastScaledTimestamp:        "",
			alarmTime:                  "2020-05-21T01:25:38.326+0000",
			evaluationPeriod:           0,
			expectedScaleStreamBoolean: true,
		},
		{
			name:                       "Invalid timestamp 1",
			lastScaledTimestamp:        invalidTimestamp1, // ""
			alarmTime:                  "2020-05-21T01:25:38.326+0000",
			evaluationPeriod:           0,
			expectedScaleStreamBoolean: true,
		},
		{
			name:                       "Invalid timestamp 2",
			lastScaledTimestamp:        invalidTimestamp2, // "abc"
			alarmTime:                  "2020-05-21T01:25:38.326+0000",
			evaluationPeriod:           0,
			expectedScaleStreamBoolean: true,
		},
	}

	// Loop through the tests
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scaleStream := checkLastScaledTimestamp(tc.lastScaledTimestamp, tc.alarmTime, tc.evaluationPeriod)
			require.Equal(t, tc.expectedScaleStreamBoolean, scaleStream,
				"Timestamp check failed. Expected scale stream boolean: %v, Actual:%v", tc.expectedScaleStreamBoolean, scaleStream)
		})
	}
}

func TestSetAlarmState(t *testing.T) {
	// Set up mocks
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCloudWatch := mockaws.NewMockCloudWatchAPI(mockController)
	svcCloudWatch = mockCloudWatch

	tests := []struct {
		name                          string
		alarmName                     string
		state                         string
		reason                        string
		expectedSetAlarmStateResponse *cloudwatch.SetAlarmStateOutput
		expectedErr                   error
	}{
		{
			name:                          "Set alarm state to OK",
			alarmName:                     "test-alarm-1",
			state:                         cloudwatch.StateValueOk,
			reason:                        "Error encountered, setting alarm state to OK",
			expectedSetAlarmStateResponse: &cloudwatch.SetAlarmStateOutput{},
			expectedErr:                   nil,
		},
		{
			name:                          "Set alarm state to INSUFFICIENT_DATA",
			alarmName:                     "test-alarm-2",
			state:                         cloudwatch.StateValueInsufficientData,
			reason:                        "Scaling done, setting alarm state to INSUFFICIENT_DATA ",
			expectedSetAlarmStateResponse: &cloudwatch.SetAlarmStateOutput{},
			expectedErr:                   nil,
		},
		{
			name:                          "Set alarm state to OK of a non-existent alarm",
			alarmName:                     "non-existent-alarm-1",
			state:                         cloudwatch.StateValueOk,
			reason:                        "Error encountered, setting alarm state to OK",
			expectedSetAlarmStateResponse: nil,
			expectedErr:                   awserr.New(cloudwatch.ErrCodeResourceNotFound, "The named resource does not exist.", errors.New("the named resource does not exist")),
		},
		{
			name:                          "Set alarm state to INSUFFICIENT_DATA of a non-existent alarm",
			alarmName:                     "non-existent-alarm-2",
			state:                         cloudwatch.StateValueOk,
			reason:                        "Scaling done, setting alarm state to INSUFFICIENT_DATA ",
			expectedSetAlarmStateResponse: nil,
			expectedErr:                   awserr.New(cloudwatch.ErrCodeResourceNotFound, "The named resource does not exist.", errors.New("the named resource does not exist")),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCloudWatch.EXPECT().SetAlarmState(&cloudwatch.SetAlarmStateInput{
				AlarmName:   aws.String(tc.alarmName),
				StateValue:  aws.String(tc.state),
				StateReason: aws.String(tc.reason),
			}).Return(tc.expectedSetAlarmStateResponse, tc.expectedErr)

			setAlarmStateResponse, err := setAlarmState(tc.alarmName, tc.state, tc.reason)
			require.Equal(t, tc.expectedSetAlarmStateResponse, setAlarmStateResponse, "Response objects are not equal")
			require.Equal(t, tc.expectedErr, err, "Error objects are not equal")
		})
	}
}

func TestGetAlarmArn(t *testing.T) {
	// Set up mocks
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCloudWatch := mockaws.NewMockCloudWatchAPI(mockController)
	svcCloudWatch = mockCloudWatch

	tests := []struct {
		name               string
		scaleUpAlarmName   string
		scaleDownAlarmName string
		scaleUpAlarmArn    string
		scaleDownAlarmArn  string
		expectedErr        error
	}{
		{
			name:               "Get Alarm ARNs",
			scaleUpAlarmName:   "scale-up-alarm",
			scaleDownAlarmName: "scale-down-alarm",
			scaleUpAlarmArn:    "arn:aws:cloudwatch:us-east-1:537561383490:alarm:scale-up-alarm",
			scaleDownAlarmArn:  "arn:aws:cloudwatch:us-east-1:537561383490:alarm:scale-down-alarm",
			expectedErr:        nil,
		},
		{
			name:               "Get alarm ARNs for non-existent alarms",
			scaleUpAlarmName:   "non-existent-scale-up-alarm",
			scaleDownAlarmName: "non-existent-scale-down-alarm",
			scaleUpAlarmArn:    "",
			scaleDownAlarmArn:  "",
			expectedErr:        nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCloudWatch.EXPECT().DescribeAlarms(&cloudwatch.DescribeAlarmsInput{
				AlarmNames: []*string{&tc.scaleUpAlarmName, &tc.scaleDownAlarmName},
			}).Return(&cloudwatch.DescribeAlarmsOutput{
				MetricAlarms: []*cloudwatch.MetricAlarm{
					{
						AlarmArn:  aws.String(tc.scaleUpAlarmArn),
						AlarmName: aws.String(tc.scaleUpAlarmName),
					},
					{
						AlarmArn:  aws.String(tc.scaleDownAlarmArn),
						AlarmName: aws.String(tc.scaleDownAlarmName),
					},
				},
			}, nil)

			scaleUpAlarmArn, scaleDownAlarmArn, err := getAlarmArn(tc.scaleUpAlarmName, tc.scaleDownAlarmName)

			require.Equal(t, tc.scaleUpAlarmArn, scaleUpAlarmArn, "Scale up alarm ARNs are not the same")
			require.Equal(t, tc.scaleDownAlarmArn, scaleDownAlarmArn, "Scale down alarm ARNs are not the same")
			require.Equal(t, tc.expectedErr, err, "Error should be nil")
		})
	}
}

func TestTagAlarm(t *testing.T) {
	// Set up mocks
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCloudWatch := mockaws.NewMockCloudWatchAPI(mockController)
	svcCloudWatch = mockCloudWatch

	tests := []struct {
		name                     string
		alarmArn                 string
		scaleAction              string
		complimentaryAlarm       string
		actionValue              string
		alarmValue               string
		lastScaledTimestamp      string
		expectedTagAlarmResponse *cloudwatch.TagResourceOutput
		expectedErr              error
	}{
		{
			name:                     "Tag the scale-up alarm",
			alarmArn:                 "arn:aws:cloudwatch:us-east-1:537561383490:alarm:scale-up-alarm",
			scaleAction:              "ScaleAction",
			complimentaryAlarm:       "ComplimentaryAlarm",
			actionValue:              "Up",
			alarmValue:               "scale-down-alarm",
			lastScaledTimestamp:      (time.Now()).Format("2006-01-02T15:04:05.000+0000"),
			expectedTagAlarmResponse: &cloudwatch.TagResourceOutput{},
			expectedErr:              nil,
		},
		{
			name:                     "Tag the scale-down alarm",
			alarmArn:                 "arn:aws:cloudwatch:us-east-1:537561383490:alarm:scale-down-alarm",
			scaleAction:              "ScaleAction",
			complimentaryAlarm:       "ComplimentaryAlarm",
			actionValue:              "Down",
			alarmValue:               "scale-up-alarm",
			lastScaledTimestamp:      (time.Now()).Format("2006-01-02T15:04:05.000+0000"),
			expectedTagAlarmResponse: &cloudwatch.TagResourceOutput{},
			expectedErr:              nil,
		},
		{
			name:                     "Tag a non-existent alarm",
			alarmArn:                 "arn:aws:cloudwatch:us-east-1:537561383490:alarm:non-existent-alarm",
			scaleAction:              "ScaleAction",
			complimentaryAlarm:       "ComplimentaryAlarm",
			actionValue:              "Down",
			alarmValue:               "non-existent-scale-up-alarm",
			lastScaledTimestamp:      (time.Now()).Format("2006-01-02T15:04:05.000+0000"),
			expectedTagAlarmResponse: nil,
			expectedErr:              awserr.New(cloudwatch.ErrCodeResourceNotFound, "The named resource does not exist.", errors.New("the named resource does not exist")),
		},
		{
			name:                     "Tag the scale-down alarm with incorrect parameters",
			alarmArn:                 "arn:aws:cloudwatch:us-east-1:537561383490:scale-down-alarm",
			scaleAction:              "ScaleAction",
			complimentaryAlarm:       "ComplimentaryAlarm",
			actionValue:              "Down",
			alarmValue:               "scale-up-alarm",
			lastScaledTimestamp:      (time.Now()).Format("2006-01-02T15:04:05.000+0000"),
			expectedTagAlarmResponse: &cloudwatch.TagResourceOutput{},
			expectedErr:              awserr.New(cloudwatch.ErrCodeInvalidParameterValueException, "The value of an input parameter is bad or out-of-range.", errors.New("the value of an input parameter is bad or out-of-range")),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCloudWatch.EXPECT().TagResource(&cloudwatch.TagResourceInput{
				ResourceARN: aws.String(tc.alarmArn),
				Tags: []*cloudwatch.Tag{
					{
						Key:   aws.String(tc.scaleAction),
						Value: aws.String(tc.actionValue),
					},
					{
						Key:   aws.String(tc.complimentaryAlarm),
						Value: aws.String(tc.alarmValue),
					},
					{
						Key:   aws.String("LastScaledTimestamp"),
						Value: aws.String(tc.lastScaledTimestamp),
					},
				},
			}).Return(tc.expectedTagAlarmResponse, tc.expectedErr)

			tagAlarmResponse, err := tagAlarm(tc.alarmArn, tc.scaleAction, tc.complimentaryAlarm, tc.actionValue, tc.alarmValue, tc.lastScaledTimestamp)

			require.Equal(t, tc.expectedTagAlarmResponse, tagAlarmResponse, "TagResource API responses are not equal")
			require.Equal(t, tc.expectedErr, err, "Error should be nil")
		})
	}
}

func TestUpdateAlarmScaleUp(t *testing.T) {
	// Set up mocks
	var periodMins int64 = 5 // Data is evaluated every 5 minutes
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCloudWatch := mockaws.NewMockCloudWatchAPI(mockController)
	svcCloudWatch = mockCloudWatch

	tests := []struct {
		name                        string
		alarmName                   string
		evaluationPeriod            int64
		datapointsRequired          int64
		threshold                   float64
		comparisonOperator          string
		streamName                  string
		alarmActions                []*string
		newShardCount               int64
		scaleDown                   bool
		scaleDownMinIterAgeMins     int64
		expectedUpdateAlarmResponse *cloudwatch.PutMetricAlarmOutput
		expectedErr                 error
	}{
		{
			name:                        "Update scale up alarm",
			alarmName:                   "scale-up-alarm",
			evaluationPeriod:            4,
			datapointsRequired:          4,
			threshold:                   0.75,
			comparisonOperator:          cloudwatch.ComparisonOperatorGreaterThanOrEqualToThreshold,
			streamName:                  "test-stream",
			alarmActions:                []*string{aws.String("arn:aws:sns:us-east-1:537561383490:test-kinesis-scaling-topic")},
			newShardCount:               32,
			scaleDown:                   false,
			scaleDownMinIterAgeMins:     0,
			expectedUpdateAlarmResponse: &cloudwatch.PutMetricAlarmOutput{},
			expectedErr:                 nil,
		},
		{
			name:                        "Update scale up alarm",
			alarmName:                   "scale-up-alarm",
			evaluationPeriod:            4,
			datapointsRequired:          4,
			threshold:                   0.75,
			comparisonOperator:          cloudwatch.ComparisonOperatorGreaterThanOrEqualToThreshold,
			streamName:                  "test-stream",
			alarmActions:                []*string{aws.String("arn:aws:sns:us-east-1:537561383490:test-kinesis-scaling-topic")},
			newShardCount:               64,
			scaleDown:                   false,
			scaleDownMinIterAgeMins:     0,
			expectedUpdateAlarmResponse: &cloudwatch.PutMetricAlarmOutput{},
			expectedErr:                 nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCloudWatch.EXPECT().PutMetricAlarm(&cloudwatch.PutMetricAlarmInput{
				AlarmName:          aws.String(tc.alarmName),
				AlarmDescription:   aws.String("Alarm to scale Kinesis stream"),
				ActionsEnabled:     aws.Bool(true),
				AlarmActions:       tc.alarmActions,
				EvaluationPeriods:  aws.Int64(tc.evaluationPeriod),
				DatapointsToAlarm:  aws.Int64(tc.datapointsRequired),
				Threshold:          aws.Float64(tc.threshold),
				ComparisonOperator: aws.String(tc.comparisonOperator),
				TreatMissingData:   aws.String("ignore"),
				Metrics: []*cloudwatch.MetricDataQuery{
					{
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
										Value: aws.String(tc.streamName),
									},
								},
							},
							Period: aws.Int64(60 * periodMins),
							Stat:   aws.String(cloudwatch.StatisticSum),
						},
					},
					{
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
										Value: aws.String(tc.streamName),
									},
								},
							},
							Period: aws.Int64(60 * periodMins),
							Stat:   aws.String(cloudwatch.StatisticSum),
						},
					},
					{
						Id:         aws.String("e1"),
						Expression: aws.String("FILL(m1,0)"),
						Label:      aws.String("FillMissingDataPointsWithZeroForIncomingBytes"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e2"),
						Expression: aws.String("FILL(m2,0)"),
						Label:      aws.String("FillMissingDataPointsWithZeroForIncomingRecords"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e3"),
						Expression: aws.String(fmt.Sprintf("e1/(1024*1024*60*%d*s1)", periodMins)),
						Label:      aws.String("IncomingBytesUsageFactor"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e4"),
						Expression: aws.String(fmt.Sprintf("e2/(1000*60*%d*s1)", periodMins)),
						Label:      aws.String("IncomingRecordsUsageFactor"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e6"),
						Expression: aws.String("MAX([e3,e4])"),
						Label:      aws.String("MaxIncomingUsageFactor"),
						ReturnData: aws.Bool(true),
					},
					{
						Id:         aws.String("s1"),
						Expression: aws.String(fmt.Sprintf("%d", tc.newShardCount)),
						Label:      aws.String("ShardCount"),
						ReturnData: aws.Bool(false),
					},
				},
			}).Return(tc.expectedUpdateAlarmResponse, tc.expectedErr).MaxTimes(20)

			updateAlarmResponse, err := updateAlarm(tc.alarmName, tc.evaluationPeriod, tc.datapointsRequired, tc.threshold, tc.comparisonOperator, tc.streamName, tc.alarmActions, tc.newShardCount, false, 0)

			require.Equal(t, tc.expectedUpdateAlarmResponse, updateAlarmResponse, "UpdateAlarm response not equal")
			require.Equal(t, tc.expectedErr, err, "Error not equal")
		})
	}
}

func TestUpdateAlarmScaleDown(t *testing.T) {
	// Set up mocks
	var periodMins int64 = 5 // Data is evaluated every 5 minutes
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCloudWatch := mockaws.NewMockCloudWatchAPI(mockController)
	svcCloudWatch = mockCloudWatch

	tests := []struct {
		name                        string
		alarmName                   string
		evaluationPeriod            int64
		datapointsRequired          int64
		threshold                   float64
		comparisonOperator          string
		streamName                  string
		alarmActions                []*string
		newShardCount               int64
		scaleDown                   bool
		scaleDownMinIterAgeMins     int64
		expectedUpdateAlarmResponse *cloudwatch.PutMetricAlarmOutput
		expectedErr                 error
	}{
		{
			name:                        "Update scale down alarm",
			alarmName:                   "scale-down-alarm",
			evaluationPeriod:            60,
			datapointsRequired:          57,
			threshold:                   0.25,
			comparisonOperator:          cloudwatch.ComparisonOperatorLessThanThreshold,
			streamName:                  "test-stream",
			alarmActions:                []*string{aws.String("arn:aws:sns:us-east-1:537561383490:test-kinesis-scaling-topic")},
			newShardCount:               8,
			scaleDown:                   true,
			scaleDownMinIterAgeMins:     30,
			expectedUpdateAlarmResponse: &cloudwatch.PutMetricAlarmOutput{},
			expectedErr:                 nil,
		},
		{
			name:                        "Update scale down alarm",
			alarmName:                   "scale-down-alarm",
			evaluationPeriod:            60,
			datapointsRequired:          57,
			threshold:                   0.25,
			comparisonOperator:          cloudwatch.ComparisonOperatorLessThanThreshold,
			streamName:                  "test-stream",
			alarmActions:                []*string{aws.String("arn:aws:sns:us-east-1:537561383490:test-kinesis-scaling-topic")},
			newShardCount:               4,
			scaleDown:                   true,
			scaleDownMinIterAgeMins:     15,
			expectedUpdateAlarmResponse: &cloudwatch.PutMetricAlarmOutput{},
			expectedErr:                 nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCloudWatch.EXPECT().PutMetricAlarm(&cloudwatch.PutMetricAlarmInput{
				AlarmName:          aws.String(tc.alarmName),
				AlarmDescription:   aws.String("Alarm to scale Kinesis stream"),
				ActionsEnabled:     aws.Bool(true),
				AlarmActions:       tc.alarmActions,
				EvaluationPeriods:  aws.Int64(tc.evaluationPeriod),
				DatapointsToAlarm:  aws.Int64(tc.datapointsRequired),
				Threshold:          aws.Float64(tc.threshold),
				ComparisonOperator: aws.String(tc.comparisonOperator),
				TreatMissingData:   aws.String("ignore"),
				Metrics: []*cloudwatch.MetricDataQuery{
					{
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
										Value: aws.String(tc.streamName),
									},
								},
							},
							Period: aws.Int64(60 * periodMins),
							Stat:   aws.String(cloudwatch.StatisticSum),
						},
					},
					{
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
										Value: aws.String(tc.streamName),
									},
								},
							},
							Period: aws.Int64(60 * periodMins),
							Stat:   aws.String(cloudwatch.StatisticSum),
						},
					},
					{
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
										Value: aws.String(tc.streamName),
									},
								},
							},
							Period: aws.Int64(60 * periodMins),
							Stat:   aws.String(cloudwatch.StatisticMaximum),
						},
					},
					{
						Id:         aws.String("e1"),
						Expression: aws.String("FILL(m1,0)"),
						Label:      aws.String("FillMissingDataPointsWithZeroForIncomingBytes"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e2"),
						Expression: aws.String("FILL(m2,0)"),
						Label:      aws.String("FillMissingDataPointsWithZeroForIncomingRecords"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e3"),
						Expression: aws.String(fmt.Sprintf("e1/(1024*1024*60*%d*s1)", periodMins)),
						Label:      aws.String("IncomingBytesUsageFactor"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e4"),
						Expression: aws.String(fmt.Sprintf("e2/(1000*60*%d*s1)", periodMins)),
						Label:      aws.String("IncomingRecordsUsageFactor"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e5"),
						Expression: aws.String(fmt.Sprintf("(FILL(m3,0)/1000/60)*(%0.5f/s2)", tc.threshold)),
						Label:      aws.String("IteratorAgeAdjustedFactor"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e6"),
						Expression: aws.String("MAX([e3,e4,e5])"),
						Label:      aws.String("MaxIncomingUsageFactor"),
						ReturnData: aws.Bool(true),
					},
					{
						Id:         aws.String("s1"),
						Expression: aws.String(fmt.Sprintf("%d", tc.newShardCount)),
						Label:      aws.String("ShardCount"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("s2"),
						Expression: aws.String(fmt.Sprintf("%d", tc.scaleDownMinIterAgeMins)),
						Label:      aws.String("IteratorAgeMinutesToBlockScaleDowns"),
						ReturnData: aws.Bool(false),
					},
				},
			}).Return(tc.expectedUpdateAlarmResponse, tc.expectedErr).MaxTimes(20)

			updateAlarmResponse, err := updateAlarm(tc.alarmName, tc.evaluationPeriod, tc.datapointsRequired, tc.threshold, tc.comparisonOperator, tc.streamName, tc.alarmActions, tc.newShardCount, true, tc.scaleDownMinIterAgeMins)

			require.Equal(t, tc.expectedUpdateAlarmResponse, updateAlarmResponse, "UpdateAlarm response not equal")
			require.Equal(t, tc.expectedErr, err, "Error not equal")
		})
	}
}

func TestUpdateAlarmRetryMechanism(t *testing.T) {
	// Set up mocks
	var periodMins int64 = 5 // Data is evaluated every 5 minutes
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCloudWatch := mockaws.NewMockCloudWatchAPI(mockController)
	svcCloudWatch = mockCloudWatch

	tests := []struct {
		name                        string
		alarmName                   string
		evaluationPeriod            int64
		datapointsRequired          int64
		threshold                   float64
		comparisonOperator          string
		streamName                  string
		alarmActions                []*string
		newShardCount               int64
		expectedUpdateAlarmResponse *cloudwatch.PutMetricAlarmOutput
		expectedErr                 error
	}{
		{
			name:                        "Update scale down alarm (API Throttle scenario)",
			alarmName:                   "scale-down-alarm",
			evaluationPeriod:            60,
			datapointsRequired:          57,
			threshold:                   0.25,
			comparisonOperator:          cloudwatch.ComparisonOperatorLessThanThreshold,
			streamName:                  "test-stream",
			alarmActions:                []*string{aws.String("arn:aws:sns:us-east-1:537561383490:test-kinesis-scaling-topic")},
			newShardCount:               4,
			expectedUpdateAlarmResponse: &cloudwatch.PutMetricAlarmOutput{},
			expectedErr:                 awserr.New(cloudwatch.ErrCodeLimitExceededFault, "The quota for alarms for this customer has already been reached.", errors.New("the quota for alarms for this customer has already been reached")),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCloudWatch.EXPECT().PutMetricAlarm(&cloudwatch.PutMetricAlarmInput{
				AlarmName:          aws.String(tc.alarmName),
				AlarmDescription:   aws.String("Alarm to scale Kinesis stream"),
				ActionsEnabled:     aws.Bool(true),
				AlarmActions:       tc.alarmActions,
				EvaluationPeriods:  aws.Int64(tc.evaluationPeriod),
				DatapointsToAlarm:  aws.Int64(tc.datapointsRequired),
				Threshold:          aws.Float64(tc.threshold),
				ComparisonOperator: aws.String(tc.comparisonOperator),
				TreatMissingData:   aws.String("ignore"),
				Metrics: []*cloudwatch.MetricDataQuery{
					{
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
										Value: aws.String(tc.streamName),
									},
								},
							},
							Period: aws.Int64(60 * periodMins),
							Stat:   aws.String(cloudwatch.StatisticSum),
						},
					},
					{
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
										Value: aws.String(tc.streamName),
									},
								},
							},
							Period: aws.Int64(60 * periodMins),
							Stat:   aws.String(cloudwatch.StatisticSum),
						},
					},
					{
						Id:         aws.String("e1"),
						Expression: aws.String("FILL(m1,0)"),
						Label:      aws.String("FillMissingDataPointsWithZeroForIncomingBytes"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e2"),
						Expression: aws.String("FILL(m2,0)"),
						Label:      aws.String("FillMissingDataPointsWithZeroForIncomingRecords"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e3"),
						Expression: aws.String(fmt.Sprintf("e1/(1024*1024*60*%d*s1)", periodMins)),
						Label:      aws.String("IncomingBytesUsageFactor"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e4"),
						Expression: aws.String(fmt.Sprintf("e2/(1000*60*%d*s1)", periodMins)),
						Label:      aws.String("IncomingRecordsUsageFactor"),
						ReturnData: aws.Bool(false),
					},
					{
						Id:         aws.String("e6"),
						Expression: aws.String("MAX([e3,e4])"),
						Label:      aws.String("MaxIncomingUsageFactor"),
						ReturnData: aws.Bool(true),
					},
					{
						Id:         aws.String("s1"),
						Expression: aws.String(fmt.Sprintf("%d", tc.newShardCount)),
						Label:      aws.String("ShardCount"),
						ReturnData: aws.Bool(false),
					},
				},
			}).Return(tc.expectedUpdateAlarmResponse, tc.expectedErr).MaxTimes(20)

			updateAlarmResponse, err := updateAlarm(tc.alarmName, tc.evaluationPeriod, tc.datapointsRequired, tc.threshold, tc.comparisonOperator, tc.streamName, tc.alarmActions, tc.newShardCount, false, 0)

			require.Equal(t, tc.expectedUpdateAlarmResponse, updateAlarmResponse, "UpdateAlarm response not equal")
			require.Equal(t, tc.expectedErr, err, "Error not equal")
		})
	}
}

func TestUpdateConcurrency(t *testing.T) {
	// Set up mocks
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockLambda := mockaws.NewMockLambdaAPI(mockController)
	lambdaClient = mockLambda

	tests := []struct {
		name                string
		newShardCount       int64
		producerFunctionArn string
		expectedResponse    *lambda.PutFunctionConcurrencyOutput
		expectedErr         error
	}{
		{
			name:                "Update concurrency for valid function",
			newShardCount:       8,
			producerFunctionArn: "arn:aws:lambda:us-east-1:346166872260:function:kinesis-cw-producer",
			expectedResponse: &lambda.PutFunctionConcurrencyOutput{
				ReservedConcurrentExecutions: aws.Int64(8),
			},
			expectedErr: nil,
		},
		{
			name:                "Update concurrency for invalid function",
			newShardCount:       16,
			producerFunctionArn: "arn:aws:lambda:us-east-1:346166872260:function:kinesis-cw-producer-invalid",
			expectedResponse:    nil,
			expectedErr:         awserr.New(lambda.ErrCodeResourceNotFoundException, "The resource specified in the request does not exist.", errors.New("the resource specified in the request does not exist")),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockLambda.EXPECT().PutFunctionConcurrency(&lambda.PutFunctionConcurrencyInput{
				FunctionName:                 &tc.producerFunctionArn,
				ReservedConcurrentExecutions: &tc.newShardCount,
			}).Return(tc.expectedResponse, tc.expectedErr)
		})

		response, err := updateConcurrency(tc.newShardCount, tc.producerFunctionArn)

		require.Equal(t, tc.expectedResponse, response, "PutFunctionConcurrency API responses are not equal")
		require.Equal(t, tc.expectedErr, err, "Error should be nil")
	}
}

func TestDeleteConcurrency(t *testing.T) {
	// Set up mocks
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockLambda := mockaws.NewMockLambdaAPI(mockController)
	lambdaClient = mockLambda

	tests := []struct {
		name                string
		producerFunctionArn string
		expectedResponse    *lambda.DeleteFunctionConcurrencyOutput
		expectedErr         error
	}{
		{
			name:                "Delete concurrency of existing function",
			producerFunctionArn: "arn:aws:lambda:us-east-1:346166872260:function:kinesis-cw-producer",
			expectedResponse:    &lambda.DeleteFunctionConcurrencyOutput{},
			expectedErr:         nil,
		},
		{
			name:                "Delete concurrency of invalid function",
			producerFunctionArn: "arn:aws:lambda:us-east-1:346166872260:function:kinesis-cw-producer",
			expectedResponse:    nil,
			expectedErr:         awserr.New(lambda.ErrCodeResourceNotFoundException, "The resource specified in the request does not exist.", errors.New("the resource specified in the request does not exist")),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockLambda.EXPECT().DeleteFunctionConcurrency(&lambda.DeleteFunctionConcurrencyInput{
				FunctionName: &tc.producerFunctionArn,
			}).Return(tc.expectedResponse, tc.expectedErr)
		})

		response, err := deleteConcurrency(tc.producerFunctionArn)

		require.Equal(t, tc.expectedResponse, response, "DeleteFunctionConcurrency API responses are not equal")
		require.Equal(t, tc.expectedErr, err, "Error should be nil")
	}
}
