import azure.cosmos.cosmos_client as cosmos_client
import inspect

environment = 'WHQ dev wap-fftsbh-dev-chn-vegctujtkpkfg web service'
cosmos_url = 'https://cdb-tsbh-dev-chn-vegctujtkpkfg.documents.azure.com:443/'
master_key = 'XP58ILePf6LSpWitpQvYH45HNK0WtisFmWizJLxECnT0VsoTrjmlniCp0tS4vSzOFlHEXTEfa375jIrvXlyr1Q=='


def get__function_name():
    """获取正在运行函数(或方法)名称"""
    return inspect.stack()[1][3]


class Cosmos:
    def __init__(self, url, master_key):
        self.client = cosmos_client.CosmosClient(url, {'masterKey': master_key})

    def insert(self, database_id, container_id, data):
        """
        新增不需要插入id主键， 会自动生成
        :param database_id:
        :param container_id:
        :param data: 需要插入的字典
        :return:
        """
        try:
            return self.client.UpsertItem("dbs/" + database_id + "/colls/" + container_id, data)
            # print('insert %s success' % container_id)
        except Exception as error:
            print(str('%s error (%s)' % (get__function_name(), error)))
            return None

    def update(self, database_id, container_id, replace_data):
        """
        update必须指定id主键
        :param database_id:
        :param container_id:
        :param replace_data: 需要更新的全文字典
        :return:
        """
        try:
            return self.client.UpsertItem("dbs/" + database_id + "/colls/" + container_id, replace_data)
            # print('update %s  success' % (container_id,))
        except Exception as error:
            print(str('%s error (%s)' % (get__function_name(), error)))

    def query(self, database_id, container_id, fields=None, query_params=None,
              order_by=None, desc=False, offset=0, limit=None, cross_partition=True):
        """
        :param database_id:
        :param container_id:
        :param fields: 需要查询的字段, tuple类型, 空值默认为全部查询，
        :param query_params:  查询条件，dict类型, 空值默认为无条件,
        :param order_by: 排序字段, 空值默认无排序
        :param desc: 是否倒序, 默认顺序，True为倒序，False为顺序
        :param offset: 偏移 int类型
        :param limit: 截断 int类型
        :param cross_partition: 是否跨分区查询，默认True支持，False不支持
        :return:items
        """
        if fields:
            map_fields = tuple(map(lambda x: container_id + '.' + x, fields))
            fields_str = ', '.join(map_fields)
            sql = 'SELECT %s FROM %s' % (fields_str, container_id)
        else:
            sql = 'SELECT * FROM %s' % container_id
        if query_params:
            sql += ' WHERE '
            for k, v in query_params.items():
                if isinstance(v, str):
                    sql += '%s.%s = "%s" and ' % (container_id, k, v)
                if isinstance(v, int):
                    sql += '%s.%s = %s and ' % (container_id, k, v)
        if sql.endswith('and '):
            sql = sql[:-5]
        if order_by:
            sql += ' ORDER BY %s.%s' % (container_id, order_by)
            if desc:
                sql += ' DESC'
        if limit:
            sql += ' OFFSET %s LIMIT %s' % (offset, limit)
            # print(sql)
        try:
            items = self.client.QueryItems("dbs/" + database_id + "/colls/" + container_id, sql,
                                           {'enableCrossPartitionQuery': cross_partition})
            return list(items)
        except Exception as e:
            print(str(e))
            return list()

    def query_by_raw(self, database_id, container_id, raw_sql, offset=0, cross_partition=True):
        print(raw_sql)
        try:
            items = self.client.QueryItems("dbs/" + database_id + "/colls/" + container_id, raw_sql,
                                           {'enableCrossPartitionQuery': cross_partition})
            return list(items)
        except Exception as e:
            print(str(e))
            return list()

    def delete(self, database_id, container_id, _id, partition_key_value):
        """

        :param database_id:
        :param container_id:
        :param _id:
        :param partition_key_value: 分区键对应的值，一般都是梯号UnitNumber
        :return:
        """
        try:
            self.client.DeleteItem("dbs/" + database_id + "/colls/" + container_id + "/docs/" + _id,
                                   {'partitionKey': partition_key_value})
            print('delete % success' % _id)
        except Exception as e:
            print(str(e))


cosmos = Cosmos(cosmos_url, master_key)



if __name__ == '__main__':
    environment = 'ChinaResGrp'
    cosmos_url1 = 'https://otispoc.documents.azure.com:443/'
    master_key1 = 'ZvmK12L4XdqZHTcWyBEUC8fK7FQw33JLWjLy6dpRy71PodVr6JpaoE9cD8nvWZsLwpvSLLPfB19XONbfqXjpLQ=='
    cosmos_local = Cosmos(cosmos_url1, master_key1)
    master = cosmos_local.query('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER')
    for i in master:
        i['PK'] = 'M'
        print(i['UnitNumber'])
        cosmos.insert('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', i)

