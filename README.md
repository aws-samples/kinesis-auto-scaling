# Kinesis Data Streams Auto Scaling

A lightweight system to automatically scale Kinesis Data Streams up and down based on throughput.

![Kinesis_Auto_Scaling](https://user-images.githubusercontent.com/85569859/121233258-788f3980-c860-11eb-825b-c857ddd13299.png)

# Event Flow 
- Step 1: Metrics flow from the `Kinesis Data Stream(s)` into `CloudWatch Metrics` (Bytes/Sec, Records/Sec)
- Step 2: Two alarms, `Scale Up` and `Scale Down`, evaluate those metrics and decide when to scale
- Step 3: When a scaling alarm triggers it sends a message to the `Scaling SNS Topic`
- Step 4: The `Scaling Lambda` processes that SNS message and…
  - Scales the `Kinesis Data Stream` up or down using UpdateShardCount
    - Scale Up events double the number of shards in the stream
    - Scale Down events halve the number of shards in the stream
  - Updates the metric math on the `Scale Up` and `Scale Down` alarms to reflect the new shard count.



# Features

1. Designed for simplicity and a minimal service footprint. 
2. Proven. This system has been battle tested, scaling thousands of production streams without issue.
3. Suitable for scaling massive amounts of streams. Each additional stream requires only 2 CloudWatch alarms.
4. Operations friendly. Everything is viewable/editable/debuggable in the console, no need to drop into the CLI to see what's going on.
5. Takes into account both ingress metrics `Records Per Second` and `Bytes Per Second` when deciding to scale a stream up or down.
6. Can optionally take into account egress needs via `Max Iterator Age` so streams that are N minutes behind (configurable) do not scale down and lose much needed Lambda processing power (Lambdas per Shard) because their shard count was reduced due to a drop in incoming traffic. 
7. Already designed out the box to work within the 10 UpdateShardCount per rolling 24 hour limit. 
8. Emits a custom CloudWatch error metric if scaling fails, you can alarm off this for added peace of mind.
9. Can optionally adjust reserved concurrency for your Lambda consumers as it scales their streams up and down. 

# Deployment

1.	Download `kinesis_scaling_cf.yaml` CloudFormation template
2.	Download `kinesis_scaling.zip` and upload it to your S3 Bucket
3.	Deploy the `kinesis_scaling_cf.yaml` CloudFormation template and point it to the `kinesis_scaling.zip` file in your S3 Bucket. 

# Testing

To generate traffic on your streams you can use [Kinesis Data Generator](https://aws.amazon.com/blogs/big-data/test-your-streaming-data-solution-with-the-new-amazon-kinesis-data-generator/).


# Modifying / Recompiling the Lambda

Simply edit the `scale.go` file as needed and run `./build` to genrate a `kinesis_scaling.zip` suitable for Lambda deployment. Go 1.15.x is recommened.
