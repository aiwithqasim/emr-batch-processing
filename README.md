### About Batch Data Pipeline:

The Wikipedia Activity data will be put into a folder in the S3 bucket. We will have PySpark code that will run on the EMR cluster. This code will fetch the data from the S3  bucket, perform filtering and aggregation on this data, and push the processed data back into  S3 in another folder. We will then use Athena to query this processed data present in S3. We will create a table on top of the processed data by providing the relevant schema and then use  ANSI SQL to query the data.

### Architecture Diagram:

![Architecture Diagram](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/gxpb963tyc7i5rf2qbw4.png)

- **Languages** - Python 
- **Package** - PySpark 
- **Services** - AWS EMR, AWS S3, AWS Athena.

### Dataset:

We'll be using the Wikipedia activity logs JSON dataset that has a huge payload comprising 15+ fields

NOTE: In our Script created we'll take two conditions into consideration that we want only those payloads where **_isRobot _** is **_False_** & user **_Country_** is from **_United Estate_**


![dataset](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/m7osv4y1on84ssmjmqdx.png)
