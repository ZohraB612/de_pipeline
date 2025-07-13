FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Copy dependenciies file into the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose the port that the uvicorn server will run on
EXPOSE 5000

# Run uvicorn server
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]