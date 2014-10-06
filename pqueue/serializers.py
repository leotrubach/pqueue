import json
from gzip import GzipFile
from six import BytesIO


def compress_bytes(s):
    """
    Compress string using GZIP method
    @param s The input string
    @type s basestring
    @return Compressed string
    """
    zbuf = BytesIO()
    zfile = GzipFile(mode='wb', compresslevel=6, fileobj=zbuf)
    zfile.write(s)
    zfile.close()
    return zbuf.getvalue()


def decompress_bytes(s):
    zbuf = BytesIO(s)
    zfile = GzipFile(mode='r', fileobj=zbuf)
    return zfile.read()


class CompressedJSONSerializer():
    def __init__(self, default=None, postload=None):
        self.default = default
        self.postload = postload

    def loads(self, s):
        r = json.loads(decompress_bytes(s))
        if callable(self.postload):
            return self.postload(r)
        return r

    def dumps(self, obj):
        return compress_bytes(json.dumps(obj, default=self.default))


class JSONSerializer():
    def __init__(self, default=None, postload=None):
        self.default = default
        self.postload = postload

    def loads(self, s):
        r = json.loads(s)
        if callable(self.postload):
            return self.postload(r)
        return r

    def dumps(self, obj):
        return json.dumps(obj, default=self.default)