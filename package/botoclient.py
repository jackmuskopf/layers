import boto3

class IamBoto:
    def __init__(self, access_key, secret_access_key):
        self.access_key = access_key
        self.secret_access_key = secret_access_key
    def __getattr__(self, attr):
        if hasattr(boto3, attr):
            def wrapper(*args, **kw):
                kw.update({'aws_access_key_id':self.access_key,'aws_secret_access_key':self.secret_access_key})
                return getattr(boto3, attr)(*args, **kw)
            return wrapper
        raise AttributeError(attr)

class BotoClient:
    def __init__(self,access_key, secret_access_key, region='us-east-1'):
        self.__boto = IamBoto(access_key,secret_access_key)
        self.region = region
        self.__attrs = dict()

    def __getattr__(self, attr):
        if attr == 'attrs':
            raise AttributeError()
        elif attr == 'boto':
            raise AttributeError()
        elif attr == 'set_region':
            raise AttributeError()
        elif attr == 'region':
            raise AttributeError()
        elif attr in self.__attrs:
            return self._attrs[attr]
        else:
            self.__attrs[attr] = self.__boto.client(attr,region_name=self.region)
            return self.__attrs[attr]

    def set_region(self, region):
        self.region = region
        self.__attrs = dict()
