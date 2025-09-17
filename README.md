# DevOps
Guideline for Setup:
Environment Setup Instructions:

Clone repository and navigate to project directory:
git clone https://github.com/hamnaraeel/cloudetl.git
cd psx-etl

Create and activate virtual environment:
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate

Install dependencies and setup pre-commit:
pip install -r requirements.txt
pre-commit install
