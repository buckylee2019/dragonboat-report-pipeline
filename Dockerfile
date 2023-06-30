FROM python:3.9-slim

# Set the working directory
WORKDIR /app

RUN pip install --upgrade revChatGPT==6.2.2 \
&& pip install --upgrade  flask

# Copy the application code
COPY ./chatgpt/app.py app.py

# Specify the command to run the API service
CMD [ "python", "app.py" ]
