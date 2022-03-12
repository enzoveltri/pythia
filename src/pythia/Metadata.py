import json


class Metadata:
    def __init__(self, headers, pks=None, cks=None, fd=None):
        self.headers = headers
        if pks is None:
            self.pks = []
        else:
            self.pks = pks
        if cks is None:
            self.cks = []
        else:
            self.cks = cks
        if fd is None:
            self.fd = []
        else:
            self.fd = fd

    def getHeaders(self):
        return self.headers

    def setHeaders(self, headers):
        self.headers = headers

    def getPKs(self):
        return self.pks

    def setPKs(self, pks):
        self.pks = pks

    def getCKs(self):
        return self.cks

    def setCKs(self, cks):
        self.cks = cks

    def getFDs(self):
        return self.fd

    def setFDs(self, fd):
        self.fd = fd

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)
