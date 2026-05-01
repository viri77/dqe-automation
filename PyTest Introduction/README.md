# Clone the repository (if needed)
git clone https://github.com/viri77/dqe-automation.git
# Change to your project directory
cd "Pytest Introduction" 

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

## Command used to run tests

# Run test integrated with reportportal

pytest tests/ --reportportal

# Run all tests
pytest tests


# Run unmarked tests
pytest tests -m unmarked --html=reports/unmarked_report.html

# Run validate_csv and not xfail tests
pytest tests -m "validate_csv and not xfail" --html=reports/validate_csv_report.html