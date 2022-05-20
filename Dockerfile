FROM ubuntu

ARG TARGETARCH

COPY kafkamirror /kafkamirror

EXPOSE 7070

ENTRYPOINT [ "/kafkamirror" ]
