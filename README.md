# Spark_Reddit_Project (SRP)

In this project, I have designed dwh model on reddit datase, this may not be a complete business model,but tried to implement dwh with few primary entities of reddit platform. 
I have research reddit application and its business flow, and identified few end points to develop my project.
Our main goal is to fetch data from reddit application through public API gateway with end points given in document references provided on reddit api, to datawarehouse model setup on spark warehouse. I have accomplished this complete task with multiple stages involved. 
1. Data Collection  from Reddit API
2. Persists the source files we call them daily increment files o S# buckets; partitioned based on file category
3. Run delta jobs from S3 to Spark warehouse using Pyspark running on multi node cluster


Below is our system design flow:


![image](https://user-images.githubusercontent.com/42261408/136721328-141b8619-a7f5-4491-b554-a8f0e9914b0a.png)


Below is our load process flow from S3 to Spark warehouse direcotry

![image](https://user-images.githubusercontent.com/42261408/136721405-f8956589-2bd0-44e9-b21f-67eb9e6e4070.png)




