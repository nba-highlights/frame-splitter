# Use Python 3.9 slim as the base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the Python requirements file
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Flask app source code to the container
COPY server.py .

# Expose the port that the Flask app will listen on
EXPOSE 5000

# Command to run the Flask app
CMD ["python", "-u" , "server.py"]
