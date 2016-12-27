-- Copyright (c) 2016 Uber Technologies, Inc.

-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:

-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.

-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
-- THE SOFTWARE.

CREATE TYPE destination_zone_config (
    zone text,
    allow_publish boolean,
    allow_consume boolean,
    always_replicate_to boolean,
    remote_extent_replica_num int,
);

ALTER TYPE destination ADD is_multi_zone boolean;
ALTER TYPE destination ADD zone_configs list<frozen<destination_zone_config>>;

CREATE TYPE consumer_group_zone_config (
    zone text,
    visible boolean,
);

ALTER TYPE consumer_group ADD is_multi_zone boolean;
ALTER TYPE consumer_group ADD active_zone text;
ALTER TYPE consumer_group ADD zone_configs list<frozen<consumer_group_zone_config>>;
