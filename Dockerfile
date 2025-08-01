FROM golang:1.21


ARG git_commit=unknown
ARG version="2.9.0"
ARG descriptive_version=unknown

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
LABEL org.cyverse.descriptive-version="$descriptive_version"

COPY . /go/src/github.com/cyverse-de/infosquito2
ENV CGO_ENABLED=0
RUN cd /go/src/github.com/cyverse-de/infosquito2 && \
    go install -v -ldflags "-X main.appver=$version -X main.gitref=$git_commit" .

EXPOSE 60000
LABEL org.label-schema.vcs-ref="$git_commit"
LABEL org.label-schema.vcs-url="https://github.com/cyverse-de/infosquito2"
LABEL org.label-schema.version="$descriptive_version"
ENTRYPOINT ["infosquito2"]
CMD ["--help"]
