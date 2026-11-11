.PHONY: venv setup generate run test dashboard clean

PYTHON = python3
VENV = .venv
BIN = $(VENV)/bin

venv:
	@echo "Creating virtual environment in $(VENV)..."
	$(PYTHON) -m venv $(VENV)
	@echo "Upgrading pip and installing requirements into $(VENV)..."
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements.txt
	@echo "Virtual environment created successfully!"
	@echo "Activate it with: source $(VENV)/bin/activate"

setup:
	@echo "Installing requirements..."
	pip install -r requirements.txt

generate:
	@echo "Generating sample dataset (1,000 events, seed=42)..."
	$(PYTHON) scripts/generate_data.py --rows 1000 --seed 42 --output data/input/clickstream_events.csv

run:
	@echo "Executing clickstream sessionization pipeline..."
	$(PYTHON) main.py --input data/input/clickstream_events.csv --users data/input/users.csv

test:
	@echo "Running automated test suite..."
	$(PYTHON) -m pytest -v

dashboard:
	@echo "Launching Streamlit dashboard..."
	streamlit run app.py

clean:
	@echo "Cleaning temporary files..."
	rm -rf .pytest_cache __pycache__ src/__pycache__ tests/__pycache__
