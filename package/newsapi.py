import json
import math
import logging
import hashlib
import dateutil
import time
from botocore.vendored import requests

logger = logging.getLogger(__name__)

class ApiException(Exception):
    pass


class NewsAPI:

    base_url = 'https://newsapi.org'

    def __init__(self, api_key, **kwargs):
        self.api_key = api_key
        self.headers = {'X-Api-Key' : self.api_key}
        self.page_size = kwargs.get('page_size', 100)

    def get(self, url):

        response = requests.get(url,headers=self.headers)
        obj = json.loads(response.content)
        if obj['status'] == 'error':
            print(obj)
            raise ApiException('Requests returned an error (status_code: {})'.format(response.status_code))
        else:
            return obj

    def sources(self, **kwargs):

        # default keyword arguments and update
        dkwargs = dict(country='us',language='en')
        dkwargs.update(kwargs)
        kwargs = dkwargs

        endpoint = 'v2/sources'
        keys = ['category','language','country']
        keys = filter(lambda x: x in kwargs, keys)
        queries = ['{}={}'.format(key,kwargs[key]) for key in keys]
        query = '&'.join(queries)
        path = '/'.join([self.base_url, endpoint])
        url = '?'.join([path,query])
        return self.get(url)


    def news_pages(self,**kwargs):
        dkwargs = dict(pageSize=self.page_size)
        dkwargs.update(kwargs)
        kwargs = dkwargs
        page_size = kwargs['pageSize']

        page = 1
        first_page = self.news(**kwargs,page=1)
        n_articles = first_page['totalResults']
        n_pages =  math.ceil(n_articles/page_size)

        # start generating pages
        yield first_page
        for page in range(2,n_pages+1):
            yield self.news(**kwargs,page=page)

    def npages(self,**kwargs):
        _kwargs = kwargs.copy()
        pageSize = _kwargs.get('pageSize',self.page_size)
        _kwargs['pageSize'] = 1
        res = self.news(**_kwargs)
        return math.ceil(res['totalResults']/pageSize)

    def news(self, **kwargs):

        def fmt_dates(obj,keys):
            date_fmt = '%Y-%m-%d'
            date_to_str = lambda x: x.strftime(date_fmt)
            for key in keys:
                if key in obj:
                    obj[key] = date_to_str(obj[key])
            return obj

        endpoint = 'v2/everything'
        dkwargs = dict(pageSize=self.page_size)
        dkwargs.update(kwargs)
        kwargs = dkwargs

        queries = list()
        kwargs = fmt_dates(kwargs,['to','from'])

        keys = ['q','domains','sources','to','from','pageSize','page']
        keys = filter(lambda x: x in kwargs, keys)
        queries = ['{}={}'.format(key,kwargs[key]) for key in keys]

        query = '&'.join(queries)

        path = '/'.join([self.base_url,endpoint])
        url = '?'.join([path,query])
        return self.get(url)
    
    
    def dev_stream(self, **kwargs):
        '''
        stream source at rate available to dev subscription
        '''
        sleeptime = kwargs.get('sleeptime',30)
        logger.info('sleeptime: {}'.format(sleeptime))
        npages = self.npages(**kwargs)
        xpages = min([npages+1, 10])
        for i in range(1,xpages+1):
            kwargs['page'] = i
            page = self.news(**kwargs)
            logger.info('page {}'.format(i))
            page_objs = [self.prep_for_db(obj) for obj in page['articles'] if obj is not None]
            
            yield self.first_key(page_objs)
            
            time.sleep(sleeptime)
            
    def first_key(self,objlist):
        res = list()
        keys = list()
        for obj in objlist:
            if obj['id'] not in keys:
                res.append(obj)
                keys.append(obj['id'])
        return res
            
    def hash_id(self, obj):
        hasher = hashlib.sha256()
        hash_str = ' | '.join([obj['title'], obj['source_id'], obj['publish_time'].isoformat()])
        hash_bytes = hash_str.encode()
        hasher.update(hash_bytes)
        return hasher.hexdigest(), hash_str
            

    def prep_for_db(self, api_obj):

        keys_to_keep = ['source_id', 'author', 'publish_time', 'title', 'description',
            'url', 'urlToImage']

        source_obj = api_obj['source']
        api_obj['source_id'] = source_obj['id']

        # timestamp to datetime
        api_obj['publish_time'] = dateutil.parser.parse(api_obj['publishedAt'])
        
        new_obj = {key:api_obj[key] for key in keys_to_keep if api_obj[key]}
        
        new_obj['id'], new_obj['hash_string'] = self.hash_id(new_obj)
        return new_obj
        
