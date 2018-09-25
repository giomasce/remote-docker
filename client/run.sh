#!/bin/bash

mkdir -p /root/.ssh
echo $SSH_KEY | base64 -d > /root/.ssh/id_rsa
echo $SSH_KNOWN_HOSTS | base64 -d > /root/.ssh/known_hosts
chmod 600 /root/.ssh/*
ssh $SSH_ADDR request-work > /root/work.sh
chmod 755 /root/work.sh
/root/work.sh
