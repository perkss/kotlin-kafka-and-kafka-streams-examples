#!/bin/bash

set -o nounset \
    -o errexit \
    -o verbose \
    -o xtrace

# Generate CA key
openssl req -new -x509 -keyout my-ca.key -out my-ca.crt -days 365 -subj '/CN=ca1.test.perlss/OU=TEST/O=PERKSS/L=London/S=LDN/C=UK' -passin pass:my-test-password -passout pass:my-test-password

for i in broker producer consumer
do
	echo $i
	# Create keystores
	keytool -genkey -noprompt \
				 -alias $i \
				 -dname "CN=$i.perkss.test, OU=TEST, O=PERKSS, L=London, S=LDN, C=UK" \
				 -keystore kafka.$i.keystore.jks \
				 -keyalg RSA \
				 -storepass my-test-password \
				 -keypass my-test-password

	# Create CSR, sign the key and import back into keystore
	keytool -keystore kafka.$i.keystore.jks -alias $i -certreq -file $i.csr -storepass my-test-password -keypass my-test-password

	openssl x509 -req -CA my-ca.crt -CAkey my-ca.key -in $i.csr -out $i-ca1-signed.crt -days 9999 -CAcreateserial -passin pass:my-test-password

	keytool -keystore kafka.$i.keystore.jks -alias CARoot -import -file my-ca.crt -storepass my-test-password -keypass my-test-password

	keytool -keystore kafka.$i.keystore.jks -alias $i -import -file $i-ca1-signed.crt -storepass my-test-password -keypass my-test-password

	# Create truststore and import the CA cert.
	keytool -keystore kafka.$i.truststore.jks -alias CARoot -import -file my-ca.crt -storepass my-test-password -keypass my-test-password

  echo "my-test-password" > ${i}_sslkey_creds
  echo "my-test-password" > ${i}_keystore_creds
  echo "my-test-password" > ${i}_truststore_creds
done