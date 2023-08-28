#! /bin/bash

pip install virtualenv
cd /var/www/non_prod_django
virtualenv env
source env/bin/activate
pip install -r requirements.txt