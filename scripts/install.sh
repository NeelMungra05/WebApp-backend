#! /bin/bash

if ! command -v python3 &> /dev/null; then
    echo "Python is not installed. Installing the latest version of the python."
    sudo apt-get install python -y
fi

if ! command -v ruby &> /dev/null; then
    echo "Ruby is not installed. Installing the latest version of the ruby."
    sudo snap install ruby --classic
fi

if ! command -v wget &> /dev/null; then
    echo "Wget is not installed. Installing the latest version of the wget."
    sudo apt-get install wget
fi

if ! command -v "codedeploy-agent" &> /dev/null; then
    echo "Codedeploy agent is not installed. Installing the codeploy agent."
    cd /tmp
    wget https://aws-codedeploy-ap-south-1.s3.ap-south-1.amazonaws.com/latest/install
    chmod +x ./install
    sudo ./install auto
fi

sudo service codedeploy-agent start
sudo systemctl enable codedeploy-agent