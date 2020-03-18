from cosmos_db import cosmos
import time
import datetime
import pandas as pd


# unit_gw_cloud_online_sql = """select value count(1) from c where c.GWToCloud=1"""
#
#
# def unit_gw_cloud_online():
#     return cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', unit_gw_cloud_online_sql)[0]
#
#
# # unit_gw_cloud_online = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', unit_gw_cloud_online_sql)[0]
#
# unit_gw_cloud_offline_sql = """select value count(1) from c where c.GWToCloud=0"""
#
#
# #
# def unit_gw_cloud_offline():
#     return cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', unit_gw_cloud_offline_sql)[0]
#
#
# # unit_gw_cloud_offline = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', unit_gw_cloud_offline_sql)[0]
#
# unit_controller_gw_offline_sql = """select value count(1) from c where c.ControllerToGW=0"""
#
#
# def unit_controller_gw_offline():
#     return cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', unit_controller_gw_offline_sql)[0]
# unit_controller_gw_offline = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', unit_controller_gw_offline_sql)[0]


# now = int(time.time())
# eight_hours_before_sql = """select value count(1) from c where c.GWToCloud=0 and c.GWToCloudChangeTime< %s""" % (now-8*3600)
#
# print(cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', eight_hours_before_sql)[0])

def get_nday_list(n):
    before_n_days = []
    for i in range(1, n + 1)[::-1]:
        before_n_days.append(str(datetime.date.today() - datetime.timedelta(days=i)))
    return before_n_days


def local2timestamp(date_time):
    """本地时间转UTC时间（-8: 00）"""
    timeArray = time.strptime(date_time, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    return timeStamp

#
# a = get_nday_list(30)
# for i in a:
#     start_datetime = i + ' 00:00:00'
#     end_datetime = i + ' 23:59:59'
#     start_timestamp = local2timestamp(start_datetime)
#     end_timestamp = local2timestamp(end_datetime)
#     failure_all_sql = """select value count(1) from c where (c.ReportTime between %s and %s)""" % (start_timestamp, end_timestamp)
#     failure_recover_sql = """select value count(1) from c where (c.Events=[]) and (c.OOSFlag!=1 and c.EntrapmentFlag!=1 and c.AbnormalOPModeFlag!=1) and  (c.ReportTime between %s and %s)""" % (start_timestamp, end_timestamp)
#     failure_all = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_FAILUREFLAG', failure_all_sql)[0]
#     failure_recover = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_FAILUREFLAG', failure_recover_sql)[0]
#     print(failure_all - failure_recover, failure_recover)


import plotly.express as px
# df = px.data.iris()
# # print(df)
aa = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/2014_usa_states.csv")
# print(aa)
# sql11 = """select value count(*) from c group by c.Eventcode"""
sql11 = """select distinct c.EventCode from c where c.FaultLevel='B'"""
# aa = [{'EventCode': 'dad'}, {'EventCode': 'wwwd'}, {'EventCode': 'zz'}]

_list = cosmos.query_by_raw('DB_TBS_HANDLER', 'SEND_FAILUREFLAG_TO_OES', sql11)
tmp_list = []
for i in _list:
    event_code = i['EventCode']
    sql_tmp = """select value count(1) from c where c.EventCode='%s' and c.FaultLevel='B'""" % event_code
    result = cosmos.query_by_raw('DB_TBS_HANDLER', 'SEND_FAILUREFLAG_TO_OES', sql_tmp)
    if result:
        if not event_code:
            event_code = '无event'
        else:
            event_code += ';'
        count = result[0]
        tmp_list.append({'event_name': event_code, 'count': count})


print(tmp_list)

cosmos.insert('DB_TBS_HANDLER', 'tsb_statistics', data={'data': tmp_list, 'type': 'ff_events'})

re = pd.DataFrame(tmp_list)
print(re)
