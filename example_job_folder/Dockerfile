FROM osgeo/gdal:ubuntu-full-3.6.3

RUN apt update -y

RUN apt install -y  python3-pip git

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt