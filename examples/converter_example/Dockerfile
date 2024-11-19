# Use an official Python runtime as a parent image
FROM --platform=linux/amd64 python:3.9

# RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9

# RUN apt-get update && apt-get install -y software-properties-common

# RUN add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"



# Install dependencies
RUN apt-get update && \
    apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    build-essential \
    python3-dev \
    git 

# Set GDAL environment variables
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

COPY ./requirements.txt /app/requirements.txt

RUN pip install --upgrade pip setuptools wheel

# RUN pip install rpy2

# RUN python -c "from rpy2.robjects.packages import importr; utils = importr('utils'); utils.install_packages('terra'); utils.install_packages('terra'); utils.install_packages('ncdf4')"

RUN pip install -r /app/requirements.txt

COPY ./converter.py /app/converter.py

# COPY ./remote_converter.py /app/remote_converter.py