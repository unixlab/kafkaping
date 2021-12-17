FROM scratch
COPY kafkaping /kafkaping
ENTRYPOINT ["/kafkaping"]
