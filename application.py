import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
from utils.cosmos_db import cosmos
import datetime
import time
import pandas as pd
import plotly.express as px

from dash.dependencies import Input, Output  # 回调


def get_nday_list(n):
    before_n_days = []
    for i in range(1, n + 1)[::-1]:
        before_n_days.append(str(datetime.date.today() - datetime.timedelta(days=i)))
    return before_n_days


dash_app = dash.Dash()
# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
# app = dash.Dash(__name__)

labels = ['GW to Cloud 在线', 'GW to Cloud 离线']

# values = [unit_gw_cloud_online(), unit_gw_cloud_offline()]
# def labels_values():
#     return [unit_gw_cloud_online(), unit_gw_cloud_offline()]


labels1 = ['Controller to GW 在线', 'Controll to GW 离线', 'Unknown']


# def labels1_values():
#     return [unit_gw_cloud_online() - unit_gw_cloud_offline(), unit_controller_gw_offline(), unit_gw_cloud_offline()]

def query_data():
    print(6666)
    gw_cloud_online_sql = """select value count(1) from c where c.GWToCloud=1"""
    gw_cloud_offline_sql = """select value count(1) from c where c.GWToCloud=0"""
    controller_gw_offline_sql = """select value count(1) from c where c.ControllerToGW=0"""
    gw_cloud_online = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', gw_cloud_online_sql)[0]
    gw_cloud_offline = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', gw_cloud_offline_sql)[0]
    controller_gw_offline = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', controller_gw_offline_sql)[
        0]
    Unknown = gw_cloud_offline
    controller_gw_online = gw_cloud_online + gw_cloud_offline - controller_gw_offline - Unknown
    # result = {'GW to Cloud 在线': unit_gw_cloud_online, 'GW to Cloud 离线': unit_gw_cloud_offline,
    #           'Controller to GW 在线': unit_controller_gw_online, 'Controll to GW 离线': controller_gw_offline,
    #           'Unknown': Unknown}
    result = [gw_cloud_online, gw_cloud_offline, controller_gw_online, controller_gw_offline, Unknown]
    return result


lables3 = ['离线8小时数量', '离线24小时数量', '离线3天数量', '离线7天数量']


def query_data1():
    print(88888)
    now = int(time.time())
    eight_hours_sql = """select value count(1) from c where c.GWToCloud=0 and c.GWToCloudChangeTime< %s""" % \
                      (now - 8 * 3600)
    twenty_four_hours_sql = """select value count(1) from c where c.GWToCloud=0 and c.GWToCloudChangeTime< %s""" % \
                            (now - 24 * 3600)
    three_days_sql = """select value count(1) from c where c.GWToCloud=0 and c.GWToCloudChangeTime< %s""" % \
                     (now - 3 * 24 * 3600)
    seven_days_sql = """select value count(1) from c where c.GWToCloud=0 and c.GWToCloudChangeTime< %s""" % \
                     (now - 7 * 24 * 3600)
    eight_hours = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', eight_hours_sql)[0]
    twenty_four = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', twenty_four_hours_sql)[0]
    three_days = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', three_days_sql)[0]
    seven_days = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', seven_days_sql)[0]
    return [eight_hours, twenty_four, three_days, seven_days]


def class_b_ff_event():
    _data = cosmos.query('DB_TBS_HANDLER', 'tsb_statistics', query_params={'type': 'class_b_ff_event'})[0]
    re = pd.DataFrame(_data['data'])
    print(1)
    return re


def class_c_ff_event():
    _data = cosmos.query('DB_TBS_HANDLER', 'tsb_statistics', query_params={'type': 'class_c_ff_event'})[0]
    re = pd.DataFrame(_data['data'])
    print(1)
    return re


# def query_data2():
#     thirty_days = get_nday_list(30)
#     for i in thirty_days


def dataframe():
    return pd.read_json(query_data())


#
# print(dataframe())
#
# dcc.Graph(id='',
#           figure=go.Figure(go.Figure(data=[go.Pie(labels=labels, values=values)],
#                                      layout=go.Layout(title='Gateway To Cloud'))))
# pie2 = dcc.Graph(id='',
#                  figure=go.Figure(go.Figure(data=[go.Pie(labels=labels1, values=values1)],
#                                             layout=go.Layout(title='Controller To Gateway'))))

# us_cities = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")
def unit_geo_data():
    sql111 = """select c.UnitNumber, c.Latitude as lat, c.Longitude as lon from c where c.PK = 'M'"""
    _aa = cosmos.query_by_raw('DB_TBS_HANDLER', 'COLLECTION_DSLOG_MASTER', sql111)
    data = pd.DataFrame(_aa)
    # print(re)
    data['lat'] = data['lat'].astype('float64')
    data['lon'] = data['lon'].astype('float64')
    return data


def serve_layout():
    geo_data = unit_geo_data()
    return html.Div([
        html.Div([
            html.Div([
                dcc.Dropdown(
                    id='xaxis-column',
                    options=[{'label': 'B类故障event', 'value': 'b_event'}, {'label': 'C类故障event', 'value': 'c_event'}],
                    # 打开页面默认值为b_event
                    value='b_event')],
                style=dict(width='48%', display='inline-block')
            )
        ]),
        dcc.Graph(id='indicator-graphic'),


        dcc.Graph(id='',
                  figure=go.Figure(data=px.scatter_mapbox(geo_data, lat="lat", lon="lon", hover_name='UnitNumber',
                                                          hover_data=["UnitNumber"],
                                                          color_discrete_sequence=["blue"], zoom=3,
                                                          height=750)).update_layout(
                      mapbox_style="open-street-map").update_layout(margin={"r": 0, "t": 100, "l": 0, "b": 0}))

    ])


dash_app.layout = serve_layout


# 回调
@dash_app.callback(
    Output('indicator-graphic', 'figure'),
    [Input('xaxis-column', 'value')])
def update_graph(xaxis_column_name):
    if xaxis_column_name == 'b_event':
        ff_data = class_b_ff_event()
    else:
        ff_data = class_c_ff_event()
    title = 'B类故障Event分类汇总' if xaxis_column_name == 'b_event' else 'C类故障Event分类汇总'
    result = dict(
        data=[go.Scatter(x=ff_data['event_name'],
                         y=ff_data['count'],
                         mode='markers',
                         marker_color=ff_data['count'],
                         )],
        layout=go.Layout(
            title=title,
            margin={'l': 40, 'b': 40, 't': 30, 'r': 0},
            hovermode='closest'
        )
    )
    return result


# dcc.Graph(id='',
#           figure=go.Figure(go.Figure(data=[go.Pie(labels=labels,
#                                                   values=unit_count[0:2])],
#                                      layout=go.Layout(title='Gateway To Cloud'))))

# dcc.Graph(id='',
#                   figure=go.Figure(
#                       go.Figure(data=[go.Pie(labels=labels1, values=unit_count[2:5])],
#                                 layout=go.Layout(title='Controller to GW'))))

# dcc.Graph(id='',
#                   figure=go.Figure([go.Bar(x=lables3, y=longtime_offline_count)],
#                                    layout=go.Layout(title='Gateway离线电梯数量统计')),
#                   )


# dcc.Graph(id='',
#           figure=go.Figure(data=go.Scatter(x=ff_data['event_name'],
#                                            y=ff_data['count'],
#                                            mode='markers',
#                                            marker_color=ff_data['count'],
#                                            )).update_layout(title='B类故障Event分类汇总')
#           )

# dcc.Graph(id='',
#           figure=go.Figure(data=px.scatter_mapbox(re, lat="lat", lon="lon", hover_name='UnitNumber',
#                                                   hover_data=["UnitNumber"],
#                                                   color_discrete_sequence=["fuchsia"], zoom=3,
#                                                   height=300)).update_layout(
#               mapbox_style="open-street-map").update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}))

# for azure start flask app
app = dash_app.server

if __name__ == '__main__':
    # app.run_server(debug=False, port=8000)
    dash_app.run_server(debug=True, port=8050)
