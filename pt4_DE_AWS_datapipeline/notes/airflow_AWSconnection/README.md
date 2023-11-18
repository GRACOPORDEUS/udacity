This enables connection options between Airflow and AWS
```
pip install apache-airflow-providers-amazon
```

# Step 1: Add AWS Credentials Connection
To create the 'aws_credentials' connection, you can use the following command:

```bash
airflow connections add aws_credentials --conn-type aws --conn-login AKIA4QE4NTH3R7EBEANN --conn-password s73eJIJRbnqRtll0/YKxyVYgrDWXfoRpJCDkcG2m
```
This command adds a new connection named 'aws_credentials' with the specified AWS credentials. Replace the placeholder values with your actual AWS Access Key ID as the login and Secret Access Key as the password.

# Step 2: Obtain AWS Credentials Connection URI

After executing this command, you should be able to retrieve the connection details using:

```bash
airflow connections get aws_credentials -o json
```
# Step 3: Create the Redshift Connection

After creating the 'aws_credentials' connection, you can proceed to set up the 'redshift' connection using a similar process.

```bash
airflow connections add redshift --conn-type redshift --conn-login awsuser --conn-password R3dsh1ft --conn-host default.859321506295.us-east-1.redshift-serverless.amazonaws.com --conn-port 5439 --conn-schema dev
```

# Step 4: Obtain Redshift Connection URI

To verify that the 'redshift' connection has been added, you can run:

```bash
airflow connections get redshift -o json
```
# Step 5: Set S3 Bucket Variable
```bash
airflow variables set s3_bucket sean-murdock
```
Replace 'sean-murdock' with your actual S3 bucket name.

# Step 6: Set S3 Prefix Variable
```bash
airflow variables set s3_prefix data-pipelines
```

Conclusion:
The provided script automates the process of setting up connections and variables in Airflow using the Airflow CLI. Ensure that you replace the placeholder values with your actual credentials and configurations. Also, uncomment the lines to execute the commands once you've reviewed and updated them.
