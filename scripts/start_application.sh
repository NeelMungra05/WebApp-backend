#! /bin/bash

cd /var/www/non_prod_django
env/Scripts/activate.sh
guvicorn Reconciliation.wsgi:application --bind 127.0.0.1:8000 --workers=5