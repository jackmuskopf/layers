import boto3
import time
import numbers
import math
from datetime import datetime, date

class DynamoDB:

    def __init__(self,**kwargs):
        client_kwargs = dict()
        for kw in ['aws_secret_access_key', 'aws_access_key_id','region_name']:
            if kw in kwargs:
                client_kwargs[kw] = kwargs[kw]
        self.client = boto3.client('dynamodb',**client_kwargs)


    def dict_to_dynamo(self, dictionary):


        def type_mapper(obj):


            is_number = lambda x: isinstance(x, numbers.Number)
            strings = [str]
            dates = [date, datetime]
            booleans = [bool]

            obj_type = type(obj)
            
            if is_number(obj):
                if math.isnan(obj):
                    return None
                else:
                    return {'N':str(obj)}
            elif obj_type in strings:
                return {'S':obj}
            elif obj_type in booleans:
                return {'BOOL':str(obj).lower()}
            elif obj_type in dates:
                return {'S':obj.isoformat()}
            elif obj_type is dict:
                return {
                    'M' : {
                        key : type_mapper(value) for key, value in obj.items()
                    }
                }
            elif obj_type is None:
                return None
            else:
                raise TypeError('Unexpected type to convert: {}'.format(obj_type))

        dynamo_obj = dict()
        for key, value in dictionary.items():
            new_val = type_mapper(value)
            if new_val is not None:
                dynamo_obj[key] = new_val
        return dynamo_obj

    def insert_one(self, table_name, dictionary):
        dynamo_obj = self.dict_to_dynamo(dictionary)
        return self.client.put_item(TableName=table_name, Item=dynamo_obj)

    def bulk_insert(self, table_name, dictionary_list, stream=False, quiet=False):
        if not type(dictionary_list) is list:
            raise Exception('dictionary_list must be a list')

        # break into chunks
        n_per = 25
        n_items = len(dictionary_list)
        if not quiet:
            print('inserting {} items'.format(n_items))
        n_batches = math.ceil(n_items/n_per)
        batches = (dictionary_list[x*n_per:(x+1)*n_per] for x in range(n_batches))

        # write each batch
        failed_to_process = list()
        for i, batch in enumerate(batches):
            write_res = self.write_batch(table_name,batch)
            unprocessed = write_res['UnprocessedItems']
            if not quiet:
                print('\rbatch done : {}'.format((i+1)*n_per),end='')
            if table_name in unprocessed:
                failed_to_process.extend(unprocessed[table_name])

        if not quiet:
            print('Failed to process {} items'.format(len(failed_to_process)))
        return failed_to_process

    def write_batch(self, table_name, dictionary_list):
        obj_list = [self.dict_to_dynamo(d) for d in dictionary_list]
        obj_list = [{'PutRequest' : {'Item' : obj}} for obj in obj_list]
        request_items = {
            table_name : obj_list
        }
        return self.client.batch_write_item(RequestItems=request_items)



    ###### READ OPERATIONS #####

    # use "#" for expression attribute names and ":" for expression attribute values

    def unpack(self,obj):
        res = dict()
        for key, value in obj.items():
            type_flag = next(iter(value))
            if type_flag == 'N':
                res[key] = float(value[type_flag])
            elif type_flag == 'M':
                res[key] = self.unpack(value[type_flag])
            else:
                res[key] = value[type_flag]
        return res


    def query(self, table, **kwargs):
        return self.query_wrapper(table, **kwargs)

    def scan(self, table, **kwargs):
        return self.scan_wrapper(table, **kwargs)

    def query_wrapper(self, table, **kwargs):
        _filter = kwargs.get('filter')
        values = kwargs.get('values')
        names = kwargs.get('names')
        pages = kwargs.get('pages')
        
        query_args = dict(TableName=table)

        if _filter is not None:
            query_args['KeyConditionExpression'] = _filter
        if values is not None:
            query_args['ExpressionAttributeValues'] = self.dict_to_dynamo(values)
        if names is not None:
            query_args['ExpressionAttributeNames'] = names

        if pages is not None:
            return self.get_npages(query_args, self.client.query, n=pages)
        else:
            raise ValueError('Queries must be limited')


    def scan_wrapper(self, table, **kwargs):
        _filter = kwargs.get('filter')
        names = kwargs.get('names')
        values = kwargs.get('values')
        pages = kwargs.get('pages')

        scan_args = dict(TableName=table)

        if _filter is not None:
            scan_args['FilterExpression'] = _filter
        if values is not None:
            scan_args['ExpressionAttributeValues'] = self.dict_to_dynamo(values)
        if names is not None:
            scan_args['ExpressionAttributeNames'] = names

        if pages is not None:
            return self.get_npages(scan_args, self.client.scan, n=pages)
        else:
            raise ValueError('Scans must be limited')

    def get_npages(self, args, method, n=1):
        is_more = True
        fn_args =args.copy()
        lastkey_args = dict()
        items = list()
        count = 0
        while is_more and count<=n:
            
            fn_args.update(lastkey_args)
            res = method(**fn_args)
            items.extend(res['Items'])
            is_more = ('LastEvaluatedKey' in res)
            if is_more:
                lastkey_args['ExclusiveStartKey'] = res['LastEvaluatedKey']
            count += 1
        return [self.unpack(item) for item in items]

    
