#!/bin/bash

docker build -t data_conversion .

docker run --rm -it -v $(PWD):/data_conversion data_conversion bash


# pip install -r requirements.txt
# pip install -e .

# work as usual (just different top level path names)
