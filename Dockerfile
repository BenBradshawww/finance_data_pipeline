FROM python:3.7

# install and initialize airflow
RUN pip install apache-airflow

# copy dags and install requirements
COPY requirements.txt .
RUN pip install --no-cache -r requirements.txt

CMD ["python", "./dags/new_attempt.py"]