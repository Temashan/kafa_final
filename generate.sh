#!/usr/bin/env bash
set -euo pipefail

PASSWORD="${PASSWORD:-123456}"
DAYS="${DAYS:-365}"
BROKERS=("kafka-0" "kafka-1" "kafka-2" "dr-kafka-0")

echo "=== Prepare directories ==="
mkdir -p ssl/ca ssl/brokers ssl/keystores
for NAME in "${BROKERS[@]}"; do
  mkdir -p "ssl/mounts/${NAME}"
done
rm -f ssl/ca/ca.key ssl/ca/ca.crt ssl/ca/ca.srl
rm -f ssl/brokers/*.crt ssl/brokers/*.csr ssl/brokers/*.key ssl/brokers/*.p12
rm -f ssl/keystores/*.jks
rm -f ssl/mounts/*/*.jks

echo "=== Create CA ==="
openssl genrsa -out ssl/ca/ca.key 4096
openssl req -new -x509 \
  -key ssl/ca/ca.key \
  -out ssl/ca/ca.crt \
  -days "${DAYS}" \
  -subj "/CN=Kafka-CA"

echo "=== Create broker certificates with SAN ==="
for NAME in "${BROKERS[@]}"; do
  SAN_FILE="$(mktemp)"

  printf 'subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1\n' "${NAME}" > "${SAN_FILE}"
  printf 'extendedKeyUsage=serverAuth,clientAuth\n' >> "${SAN_FILE}"

  openssl genrsa -out "ssl/brokers/${NAME}.key" 2048
  openssl req -new \
    -key "ssl/brokers/${NAME}.key" \
    -out "ssl/brokers/${NAME}.csr" \
    -subj "/CN=${NAME}"

  openssl x509 -req \
    -in "ssl/brokers/${NAME}.csr" \
    -CA ssl/ca/ca.crt \
    -CAkey ssl/ca/ca.key \
    -CAcreateserial \
    -out "ssl/brokers/${NAME}.crt" \
    -days "${DAYS}" \
    -extfile "${SAN_FILE}"

  openssl pkcs12 -export \
    -in "ssl/brokers/${NAME}.crt" \
    -inkey "ssl/brokers/${NAME}.key" \
    -out "ssl/brokers/${NAME}.p12" \
    -name "${NAME}" \
    -CAfile ssl/ca/ca.crt \
    -caname root \
    -password "pass:${PASSWORD}"

  keytool -importkeystore \
    -deststorepass "${PASSWORD}" \
    -destkeypass "${PASSWORD}" \
    -deststoretype JKS \
    -destkeystore "ssl/keystores/${NAME}.keystore.jks" \
    -srckeystore "ssl/brokers/${NAME}.p12" \
    -srcstoretype PKCS12 \
    -srcstorepass "${PASSWORD}" \
    -alias "${NAME}" \
    -noprompt

  cp "ssl/keystores/${NAME}.keystore.jks" "ssl/mounts/${NAME}/kafka.keystore.jks"
  rm -f "${SAN_FILE}"
done

echo "=== Create shared truststore ==="
keytool -keystore ssl/keystores/kafka.truststore.jks \
  -storetype JKS \
  -alias CARoot \
  -import \
  -file ssl/ca/ca.crt \
  -storepass "${PASSWORD}" \
  -noprompt

for NAME in "${BROKERS[@]}"; do
  cp ssl/keystores/kafka.truststore.jks "ssl/mounts/${NAME}/kafka.truststore.jks"
done

echo "Certificates were generated in ssl/ and ssl/keystores/"
