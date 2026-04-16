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

# Command used to run tests
-locally
pytest tests -m "parquet_data" --db_host="localhost" --db_port="5434" --db_name="mydatabase" --db_user="myuser" --db_password="mypassword" 

Jenkins
