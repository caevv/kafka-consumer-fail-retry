FROM golang:1.11-alpine3.8

ARG LIBRESSL_VERSION=2.7
ARG LIBRDKAFKA_VERSION=0.11.4-r1
ARG ALPINE_VERSION=v3.8

RUN apk update && \
    apk add libressl${LIBRESSL_VERSION}-libcrypto libressl${LIBRESSL_VERSION}-libssl --update-cache --repository http://nl.alpinelinux.org/alpine/${ALPINE_VERSION}/main && \
    apk add librdkafka=${LIBRDKAFKA_VERSION} --update-cache --repository http://nl.alpinelinux.org/alpine/${ALPINE_VERSION}/community && \
    apk add librdkafka-dev=${LIBRDKAFKA_VERSION} --update-cache --repository http://nl.alpinelinux.org/alpine/${ALPINE_VERSION}/community && \
    apk add git openssh openssl yajl-dev zlib-dev cyrus-sasl-dev openssl-dev build-base coreutils
