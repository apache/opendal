# Key Maintenance

## Check Cert

```shell
openssl x509 -in ca.crt -text -noout
```

## Generate a new CA Cert

```shell
# Generate a new CA key
openssl genrsa -out ca.key 2048

# Generate a new CA cert which valid for 100 years
openssl req -x509 -new -nodes -key ca.key -sha256 -days 36500 -out ca.crt -subj "/C=US/O=Apache OpenDAL Service Test Redis/CN=redis.test.service.opendal.apache.org"

# Check the cert
openssl x509 -in ca.crt -text -noout
```

## Generate a new Peer Cert

```shell
# Generate a new perr key
openssl genrsa -out redis.key 2048

# Generate a new CSR
openssl req -new -key redis.key -out redis.csr -config req.conf

# Use CA Cert to sign the CSR
openssl x509 -req -in redis.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out redis.crt -days 36500 -sha256 -extfile req.conf -extensions v3_ca

# Check the cert
openssl x509 -in redis.crt -text -noout
```
