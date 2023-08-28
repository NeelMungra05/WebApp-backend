#! /bin/bash

pip install virtualenv
cd /var/www/non_prod_django
virtualenv env
env/Scripts/activate.sh
pip install -r requirements.txt