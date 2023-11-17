#Step 1: Obtain AWS Credentials Connection URI
```bash
airflow connections get aws_credentials -o json
```
Copy the 'get_uri' value from the JSON output.

#Step 2: Add AWS Credentials Connection
```bash
airflow connections add aws_credentials --conn-uri 'aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@'
```
Replace the connection URI with the one copied in #Step 1.

#Step 3: Obtain Redshift Connection URI
```bash
airflow connections get redshift -o json
```
Copy the 'get_uri' value from the JSON output.

#Step 4: Add Redshift Connection
```bash
airflow connections add redshift --conn-uri 'redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
```
Replace the connection URI with the one copied in #Step 3.

#Step 5: Set S3 Bucket Variable
```bash
airflow variables set s3_bucket sean-murdock
```
Replace 'sean-murdock' with your actual S3 bucket name.

#Step 6: Set S3 Prefix Variable
```bash
airflow variables set s3_prefix data-pipelines
```
Uncomment this line if you want to use the specified prefix.

Conclusion:
The provided script automates the process of setting up connections and variables in Airflow using the Airflow CLI. Ensure that you replace the placeholder values with your actual credentials and configurations. Also, uncomment the lines to execute the commands once you've reviewed and updated them.
