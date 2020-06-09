FROM infinivision/build as builder

COPY . /opt/app-root/src/go/src/github.com/infinivision/hyena
WORKDIR /opt/app-root/src/go/src/github.com/infinivision/hyena

RUN make hyena

FROM infinivision/centos
COPY --from=builder /opt/app-root/src/go/src/github.com/infinivision/hyena/dist/hyena /usr/local/bin/hyena

ENTRYPOINT ["/usr/local/bin/hyena"]