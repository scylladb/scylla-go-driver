#!/bin/bash
# This script generates a self signed x.509 certificate with subject alternative names
# specified in db.cfg.

openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -days 3650 -config db.cfg  -out cadb.pem
openssl genrsa -out db.key 4096
openssl req -new -key db.key -out db.csr -config db.cfg -reqexts req_ext 
openssl x509 -req -in db.csr -CA cadb.pem -CAkey ca.key -CAcreateserial -out db.crt -days 3650 -extfile db.cfg -extensions req_ext
rm db.csr cadb.srl ca.key
