FROM hub.bccvl.org.au/bccvl/python-gdal:3.6-1-alpine

# install rclone
RUN curl -LO https://downloads.rclone.org/rclone-current-linux-amd64.zip \
  && unzip rclone-current-linux-amd64.zip \
  && rm -f rclone-current-linux-amd64.zip \
  && cd rclone-*-linux-amd64 \
  && cp rclone /usr/bin \
  && chmod 755 /usr/bin/rclone \
  # && mkdir -p /usr/share/man/man1 \
  # && cp rclone.1 /usr/share/man/man1 \
  # && makewhatis /usr/share/man \
  && cd .. \
  && rm -fr rclone-*-linux-amd64

COPY requirements.txt /tmp

# Install other packages
RUN pip3 install --no-cache python-openstackclient python-swiftclient
RUN pip3 install --no-cache -r /tmp/requirements.txt

WORKDIR /data_conversion

CMD /bin/sh
