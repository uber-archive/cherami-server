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

FROM golang:1.7.4

# get and compile cherami-server
ENV CHERAMI_HOME $GOPATH/src/github.com/uber/cherami-server

RUN go get -u github.com/Masterminds/glide
RUN git clone --depth=50 https://github.com/uber/cherami-server.git $CHERAMI_HOME
RUN cd $CHERAMI_HOME; make bins

EXPOSE 4922 4253 4240 4254 5425 6280 6189 6190 6191 6310

WORKDIR $CHERAMI_HOME

# Remember to mount host directories (config and data), and have CHERAMI_CONFIG_DIR properly point to config.
CMD ["./cherami-server", "start", "all"]
