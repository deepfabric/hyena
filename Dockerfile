FROM infinivision/build as builder

COPY . /root/go/src/github.com/infinivision/hyena
WORKDIR /root/go/src/github.com/infinivision/hyena

RUN make hyena

FROM infinivision/centos
COPY --from=builder /root/go/src/github.com/infinivision/hyena/dist/hyena /usr/local/bin/hyena

ENTRYPOINT ["/usr/local/bin/hyena"]