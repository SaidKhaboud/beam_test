# Use the official Python image from Docker Hub
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install required dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the pipeline code into the container at /app
COPY pipeline.py .

# Run the Beam pipeline when the container starts
CMD ["python", "pipeline.py"]
