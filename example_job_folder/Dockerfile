FROM osgeo/gdal:ubuntu-full-3.6.3

RUN apt update -y

RUN apt install -y  python3-pip git libhdf5-dev

COPY ./requirements.txt /app/requirements.txt

RUN pip install --upgrade pip setuptools wheel

RUN pip install -r /app/requirements.txt