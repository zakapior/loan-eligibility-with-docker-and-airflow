FROM python:3.12-slim
RUN python -m pip install \
        kaggle==1.5.16 \
        pandas==2.1.3 \
        sqlalchemy==2.0.23 \
        psycopg2-binary
WORKDIR /usr/src/app
COPY download_data.py .
COPY transform_and_load_data.py .