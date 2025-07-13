FROM prefecthq/prefect:2.19.3-python3.10

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application files
COPY . .

# Command to run worker deployment and start the worker
CMD ["sh", "-c", "python worker.py && prefect worker start --pool default"]