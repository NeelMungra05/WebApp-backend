#! /bin/bash

if [ -d "/var/www/non_prod_django" ]; then
    echo "Removing old application from /var/www/non_prod_django folder..."
    sudo rm -rf /var/www/non_prod_django
else
    echo "/var/www/non_prod_django folder not found"
fi