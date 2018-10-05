FROM alpine
COPY /nsq-to-nsq /usr/local/bin/nsq-to-nsq
COPY /nsq-to-http /usr/local/bin/nsq-to-http
COPY /nsqlookup-proxy /usr/local/bin/nsqlookup-proxy
