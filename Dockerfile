FROM python:3.7

WORKDIR /usr/src/app

COPY ./requirements.txt /usr/src/app/requirements.txt

RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt 

COPY ./streamlit /usr/src/app/streamlit
COPY ./scripts /usr/src/app/scripts

CMD ["streamlit", "run", "streamlit/main.py", "--server.port=8501", "--server.address=0.0.0.0"]