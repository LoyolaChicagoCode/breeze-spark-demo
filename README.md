Synopsis
---------

This is a simple code to demonstrate the use of Breeze (a linear algebra
package) Scala in an Apache Spark clustered environment.

In one possible application, we do analytics on 2D layers. This code is 
not intended to be completely realistic but does show a few things:

- Working with 3D data (Array of DenseMatrix)
- Performing an aggregate operation on each layer (matrix trace for now)
- Performing a collective operation (sum of traces)

Running
---------

- `sbt compile`

- `sbt assembly` - This builds the jar in target/scala-2.10/demo-breeze-spark-scala-assembly-1.0.jar

- `spark-submit --master  target/scala-2.10/demo-breeze-spark-scala-assembly-1.0.jar [--dim dimension ] [--nodes 4] [--partitions 48] [--workload 1024]

   --dim: Used to specify the dimension (this results in dimension^3 units of storage so make sure to set your JVM memory in Spark accordingly.

   --nodes: Number of nodes you are using in your cluster. This is not used yet but will allow us to compute the actual utilization/throughput. I am still working to determine whether I can find this accurately from the Spark Context.

   --partitions: How to  partition the RDD.

   --workload: Number of computations you want to do. By default, we use nodes * partitions so there is sufficient work to do per node and (hopefully) take advantage of cores.


