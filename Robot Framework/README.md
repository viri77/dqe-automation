# Clone the repository (if needed)
git clone https://github.com/viri77/dqe-automation.git
# Change to your project directory
cd "Robot Framework"

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

# Run test
robot --outputdir ./results test.robot
