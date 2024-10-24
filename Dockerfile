FROM python:3.7

WORKDIR /usr/src/app

# dont write pyc files
# dont buffer to stdout/stderr

COPY ./requirements.txt /usr/src/app/requirements.txt

# dependencies
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt 

COPY ./streamlit /usr/src/app