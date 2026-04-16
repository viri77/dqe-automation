# Clone the repository (if needed)
git clone https://github.com/viri77/dqe-automation.git
# Change to your project directory
cd "Pytest DQ Framework" 

## Setup

# Create virtual environment
Mac
python3 -m venv .venv

windows
python -m venv venv

# Activate virtual environment
Mac
source .venv/bin/activate

Windows
venv\Scripts\activate

# Install requirements
pip install -r requirements.txt

# in order to run tests locally need to copy parquet files to repo:
cd ..  (dqe-atomation folder)
```
podman-compose up -d
```
Check that the containers are running and connected to the same network:

```
podman ps
```
Copy files
```
podman cp jenkins:/parquet_data "PyTest DQ Framework/parquet_data"
```


# Command used to run tests
-locally
pytest tests -m "parquet_data" --db_host="localhost" --db_port="5434" --db_name="mydatabase" --db_user="myuser" --db_password="mypassword" 

## Access Jenkins

Open your browser and navigate to: http://localhost:8080

### Retrieve Initial Admin Password

From a file:

```
jenkins_home\secrets\initialAdminPassword
```

From logs:

```
podman logs jenkins | grep "Please use the following password"
```

### First initialization

1) Login using password from previous step to start initializing and click on “Install suggested plugins”.
2) Wait until all plugins are installed.
3) Create first admin user (remember credentials).

## Run data pipeline

1. Create a New Pipeline Job:

* In Jenkins, click New Item.
* Select Pipeline and give it a name.
* Click OK.

2. Configure the Pipeline:

* Under Pipeline, select Pipeline script from SCM.
* Choose Git as the SCM.
* Enter the URL of your GitHub repository (e.g., https://github.com/viri77/dqe-automation.git)
* Specify the branch to use (e.g., main).
* Specify Script Path to Jenkins file: PyTest DQ Framework/Jenkinsfile. 

3. Build the Pipeline.

4. Verify result: check report
