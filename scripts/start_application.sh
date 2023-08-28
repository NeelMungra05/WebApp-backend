#! /bin/bash

cd /var/www/non_prod_django
source env/lib/activate.sh
gunicorn Reconciliation.wsgi:application --bind 127.0.0.1:8000 --workers=5