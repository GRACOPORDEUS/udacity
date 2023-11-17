# Installing Apache Airflow on Ubuntu

If you want to install Apache Airflow on Ubuntu using the terminal, you can follow these steps:

## Install Dependencies

Open a terminal and run the following command to install the required dependencies:

```
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev
sudo apt-get install -y libssl-dev libffi-dev libmysqlclient-dev libgmp3-dev
Install Apache Airflow
```

## Run the following command to install Apache Airflow using pip:

```
pip3 install apache-airflow
```

## Initialize Airflow Database

After installing Airflow, initialize the database by running the following commands:

```
airflow db init
```

## Start the Airflow Web Server

Start the Airflow web server by running the following command:
```
airflow webserver --port 8080
```

The web server should now be accessible at http://localhost:8080 in your web browser.
Start the Scheduler

Open a new terminal and run the following command to start the scheduler:
```
airflow scheduler
```

The scheduler is responsible for scheduling and triggering the tasks defined in your DAGs.

That's it! You should now have Apache Airflow installed and running locally on your Ubuntu machine. You can use the web interface to manage and monitor your workflows.

Remember that these instructions assume you have Python and pip installed on your Ubuntu machine. If you encounter any issues during installation, you may need to troubleshoot and install additional dependencies based on your system configuration