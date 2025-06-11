# Your own Data Stack

This is a repo where we show off how to operate a Data-Stack that follows modern Software-Engineering and Data-Engineering Standards.

The best part: You can completely self-host it without moving your Data outside of your Organisation.

*Note*: Since we use the same Standards as the Managed Services, each Component can also easily be integrated into your Cloud Systems.

Imagine you are ingestin Data from your On-Premises Systems into your Spark environment.
If you always compute the full load - your Data Stack will not scale: Imagine you run your Data-Stack for 1 year, you would have to compute the full load for every year on each day.
If you are running your Data Stack for 10 Years. Your computation would take approx 10x to execute.

Now if you integrate Spark Structured Streaming (which is not much different than traditional APIs), you can run your computation each day, and it will only calculate the changes. This scales.


# This is fully open source.
1) For storage you can use any S3 or Hadoop compatible System. Use your managed service or bootstrap your own Minio Ceph Cluster.
2) For orchestration you can use Airflow running (also managed or in your own Kubernetes Cluster)
3) Since its delta Tables you can integrate Data with the Delta-Sharing Protocol or sync the stream to hot storage Systems Like a Relational DB to serve your users.

# Next Steps:
- Airflow Orchetstration
- Check out the Data Lineage with Airflow Datasets.


