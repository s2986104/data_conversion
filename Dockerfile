#FROM python:3.6
FROM debian:buster

RUN export DEBIAN_FRONTEND=noninteractive \
 && apt-get update \
 && apt-get upgrade -y \
 #&& apt-get install -y --no-install-recommends \
 #     gdal-bin \
 #     libgdal-dev \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gdal-bin \
      python3 \
      python3-distutils \
      python3-gdal

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
 && python3 get-pip.py \
 && rm get-pip.py


# Install python gdal bindings
#RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal \
# && export C_INCLUDE_PATH=/usr/include/gdal \
# && pip install --no-cache-dir GDAL==2.1.3

# Install other stuff needed for supporting things
RUN export DEBIAN_FRONTEND=noninteractive \
 && apt-get install -y --no-install-recommends gcc python3-dev \
 && pip install python-openstackclient python-swiftclient \
 && apt-get install -y --no-install-recommends unzip man-db \
 && curl https://rclone.org/install.sh | bash

WORKDIR /data_conversion

# xarray? numpy? rasterio?
# cd /data-conversion/
# pip3 install -r requirements.txt
