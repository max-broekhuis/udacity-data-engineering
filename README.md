# Data Pipelines with Airflow
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Setup - to run locally

1. Install Airflow, create variable AIRFLOW_HOME and AIRFLOW_CONFIG with the appropiate paths, and place dags and plugins on airflow_home directory.
2. Initialize Airflow data base with `airflow initdb`, and open webserver with `airflow webserver`
3. Access the server `http://localhost:8080` and create:

**AWS Connection**
Conn Id: Enter aws_credentials.
Conn Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

**Redshift Connection**
Conn Id: Enter redshift.
Conn Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. 
Schema: This is the Redshift database you want to connect to.
Login: Enter awsuser.
Password: Enter the password created when launching the Redshift cluster.
Port: Enter 5439.

## Data Source 
* Log data: `s3://udacity-dend/log_data`
* Song data: `s3://udacity-dend/song_data`

## Project Structure
* README: Instructions and documentation

### Operators
Operators create necessary tables, stage the data, transform the data, and run checks on data quality.

Connections and Hooks are configured using Airflow's built-in functionalities.

All of the operators and task run SQL statements against the Redshift database. 

#### Stage Operator
The stage operator loads any JSON and CSV formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

- **Task to stage CSV and JSON data is included in the DAG and uses the RedshiftStage operator**: There is a task that to stages data from S3 to Redshift. (Runs a Redshift copy statement)

- **Task uses params**: Instead of running a static SQL statement to stage the data, the task uses params to generate the copy statement dynamically. It also contains a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

- **Logging used**: The operator contains logging in different steps of the execution

- **The database connection is created by using a hook and a connection**: The SQL statements are executed by using a Airflow hook

#### Fact and Dimension Operators
The dimension and fact operators make use of the SQL helper class to run data transformations. Operators use SQL statament from helper to target the database. Output results in a transformed target table.

Dimension loads follow truncate-insert function. 


#### Data Quality Operator
The data quality operator is used to run checks on the data itself. Testing is done with SQL query. 

