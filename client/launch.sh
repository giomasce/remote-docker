#!/bin/bash

TEMP=$(mktemp)

echo -n "SSH_KEY=" > "$TEMP"
cat id_rsa | base64 -w 0 >> "$TEMP"
echo >> "$TEMP"
echo -n "SSH_KNOWN_HOSTS=" >> "$TEMP"
cat known_hosts | base64 -w 0 >> "$TEMP"
echo >> "$TEMP"
echo -n "SSH_ADDR=" >> "$TEMP"
cat ssh_addr >> "$TEMP"

docker run --rm --env-file "$TEMP" giomasce/remote-docker /root/run.sh

rm "$TEMP"
