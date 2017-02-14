# Copyright (c) 2016 Uber Technologies, Inc.

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

FROM cassandra:3.9

# get golang 1.7.4 (https://github.com/docker-library/golang/blob/18ee81a2ec649dd7b3d5126b24eef86bc9c86d80/1.7/Dockerfile)
RUN apt-get update && apt-get install -y --no-install-recommends \
		g++ \
		gcc \
		libc6-dev \
		make \
		pkg-config \
		curl \
		git libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev gettext \
	&& rm -rf /var/lib/apt/lists/*

ENV GOLANG_VERSION 1.7.4
ENV GOLANG_DOWNLOAD_URL https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 47fda42e46b4c3ec93fa5d4d4cc6a748aa3f9411a2a2b7e08e3a6d80d753ec8b

RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

# get and compile cherami-server
ENV CHERAMI_HOME $GOPATH/src/github.com/uber/cherami-server

RUN go get -u github.com/Masterminds/glide
RUN git clone --depth=50 https://github.com/uber/cherami-server.git $CHERAMI_HOME
RUN cd $CHERAMI_HOME; make bins

VOLUME /var/lib/cherami-store
EXPOSE 4922 4253 4240 4254 5425 6280 6189 6190 6191 6310

COPY ./start.sh $CHERAMI_HOME/start.sh
COPY ./healthcheck.sh $CHERAMI_HOME/healthcheck.sh
COPY ./docker_template.yaml $CHERAMI_HOME/config/docker_template.yaml
RUN chmod a+x $CHERAMI_HOME/start.sh
RUN chmod a+x $CHERAMI_HOME/healthcheck.sh

WORKDIR $CHERAMI_HOME
CMD ./start.sh

HEALTHCHECK --interval=1s --timeout=1s --retries=120 CMD ./healthcheck.sh
