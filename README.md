# AWS-lambda-S3-to-Elastic-Indexing-Connector
AWS Lambda code to index S3 buckets into Elasticsearch

A work in progress code - 

This is a AWS lambda java code that will be triggered everytime there is a change in the S3 bucket. THis uses the Apache Tika library to read the binary files. This indexes the files and it's meta-data into AWS Elasticsearch service.

I will soon update the mapping for the Elastic Index, with the synonyms feature.

### As always, don't forget to improvise!
