# Makefile to automate common tasks
.PHONY: install lint test run

# Install dependencies
install:
    pip install -r requirements.txt

# Run pre-commit hooks
lint:
    pre-commit run --all-files

# Run tests with pytest
test:
    pytest tests/

# Run the FastAPI server
run:
    uvicorn services/extract.app:app --reload
    