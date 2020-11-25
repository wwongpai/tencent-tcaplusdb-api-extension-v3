#!/usr/bin/python3

import os
import sys
import requests
import json
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.monitor.v20180724 import monitor_client, models
from datetime import datetime, timedelta
from glom import glom


def createMetricData(metric_name,inst_id1,table_id1,values_1,inst_id2,table_id2,values_2,inst_id3,table_id3,values_3,inst_id4,table_id4,values_4,inst_id5,table_id5,values_5,inst_id6,table_id6,values_6,inst_id7,table_id7,values_7,inst_id8,table_id8,values_8,inst_id9,table_id9,values_9,inst_id10,table_id10,values_10):
    metricData =  [
                    { "metricName": f"Custom Metrics|TcaplusDB|{inst_id1}|{table_id1}|{metric_name}",
                      "aggregatorType": "OBSERVATION",
                      "value": values_1 },
                    { "metricName": f"Custom Metrics|TcaplusDB|{inst_id2}|{table_id2}|{metric_name}",
                      "aggregatorType": "OBSERVATION",
                      f"value": values_2 },
                    { "metricName": f"Custom Metrics|TcaplusDB|{inst_id3}|{table_id3}|{metric_name}",
                      "aggregatorType": "OBSERVATION",
                      "value": values_3 },
                    { "metricName": f"Custom Metrics|TcaplusDB|{inst_id4}|{table_id4}|{metric_name}",
                      "aggregatorType": "OBSERVATION",
                      f"value": values_4 },
                    { "metricName": f"Custom Metrics|TcaplusDB|{inst_id5}|{table_id5}|{metric_name}",
                      "aggregatorType": "OBSERVATION",
                      "value": values_5 },
                    { "metricName": f"Custom Metrics|TcaplusDB|{inst_id6}|{table_id6}|{metric_name}",
                      "aggregatorType": "OBSERVATION",
                      f"value": values_6 },
                    { "metricName": f"Custom Metrics|TcaplusDB|{inst_id7}|{table_id7}|{metric_name}",
                      "aggregatorType": "OBSERVATION",
                      "value": values_7 },
                    { "metricName": f"Custom Metrics|TcaplusDB|{inst_id8}|{table_id8}|{metric_name}",
                      "aggregatorType": "OBSERVATION",
                      f"value": values_8 },
                    { "metricName": f"Custom Metrics|TcaplusDB|{inst_id9}|{table_id9}|{metric_name}",
                      "aggregatorType": "OBSERVATION",
                      "value": values_9 },
                    { "metricName": f"Custom Metrics|TcaplusDB|{inst_id10}|{table_id10}|{metric_name}",
                      "aggregatorType": "OBSERVATION",
                      f"value": values_10 },                                                                                                              
                   ]
    return metricData

cwd = os.getcwd()
with open(f"{cwd}/dimension.txt", "r") as file:
  data = file.readlines()

arr_data = []
for i in data:
  arr_data.append(i.strip())

ins1 = arr_data[14].rsplit(' ',1)[1]
tbl1 = arr_data[15].rsplit(' ',1)[1]
ins2 = arr_data[17].rsplit(' ',1)[1]
tbl2 = arr_data[18].rsplit(' ',1)[1]
ins3 = arr_data[20].rsplit(' ',1)[1]
tbl3 = arr_data[21].rsplit(' ',1)[1]
ins4 = arr_data[23].rsplit(' ',1)[1]
tbl4 = arr_data[24].rsplit(' ',1)[1]
ins5 = arr_data[26].rsplit(' ',1)[1]
tbl5 = arr_data[27].rsplit(' ',1)[1]
ins6 = arr_data[29].rsplit(' ',1)[1]
tbl6 = arr_data[30].rsplit(' ',1)[1]
ins7 = arr_data[32].rsplit(' ',1)[1]
tbl7 = arr_data[33].rsplit(' ',1)[1]
ins8 = arr_data[35].rsplit(' ',1)[1]
tbl8 = arr_data[36].rsplit(' ',1)[1]
ins9 = arr_data[38].rsplit(' ',1)[1]
tbl9 = arr_data[39].rsplit(' ',1)[1]
ins10 = arr_data[41].rsplit(' ',1)[1]
tbl10 = arr_data[42].rsplit(' ',1)[1]
secretid = arr_data[4].rsplit(' ',1)[1]
secretkey = arr_data[7].rsplit(' ',1)[1]
endpointurl = arr_data[1].rsplit(' ',1)[1]
region = arr_data[10].rsplit(' ', 1)[1]

try:
    cred = credential.Credential(secretid,secretkey) 
    httpProfile = HttpProfile()
    httpProfile.endpoint = "monitor.tencentcloudapi.com"

    clientProfile = ClientProfile()
    clientProfile.httpProfile = httpProfile
    client = monitor_client.MonitorClient(cred, region, clientProfile) 

    req = models.GetMonitorDataRequest()


    time1 = datetime.now()
    time2 = datetime.now() + timedelta(minutes=-5)
    c_time1 = time1.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    c_time2 = time2.strftime("%Y-%m-%dT%H:%M:%S+08:00")

    metric_list = ["Avgerror", "WriteLatency", "Comerror", "ReadLatency", "Volume", "Syserror", "Writecu", "Readcu"]
    for i in range(len(metric_list)):
      # print(metric_list[i])

      params = {
          "Instances": [
              {
                  "Dimensions": [
                      {
                          "Name": "ClusterId",
                          "Value": ins1
                      },
                      {
                          "Name": "TableInstanceId",
                          "Value": tbl1
                      }
                  ]
              },
              {
                  "Dimensions": [
                      {
                          "Name": "ClusterId",
                          "Value": ins2
                      },
                      {
                          "Name": "TableInstanceId",
                          "Value": tbl2
                      }
                  ]
              },
              {
                  "Dimensions": [
                      {
                          "Name": "ClusterId",
                          "Value": ins3
                      },
                      {
                          "Name": "TableInstanceId",
                          "Value": tbl3
                      }
                  ]
              },
              {
                  "Dimensions": [
                      {
                          "Name": "ClusterId",
                          "Value": ins4
                      },
                      {
                          "Name": "TableInstanceId",
                          "Value": tbl4
                      }
                  ]
              },
              {
                  "Dimensions": [
                      {
                          "Name": "ClusterId",
                          "Value": ins5
                      },
                      {
                          "Name": "TableInstanceId",
                          "Value": tbl5
                      }
                  ]
              },
              {
                  "Dimensions": [
                      {
                          "Name": "ClusterId",
                          "Value": ins6
                      },
                      {
                          "Name": "TableInstanceId",
                          "Value": tbl6
                      }
                  ]
              },
              {
                  "Dimensions": [
                      {
                          "Name": "ClusterId",
                          "Value": ins7
                      },
                      {
                          "Name": "TableInstanceId",
                          "Value": tbl7
                      }
                  ]
              },
              {
                  "Dimensions": [
                      {
                          "Name": "ClusterId",
                          "Value": ins8
                      },
                      {
                          "Name": "TableInstanceId",
                          "Value": tbl8
                      }
                  ]
              },
              {
                  "Dimensions": [
                      {
                          "Name": "ClusterId",
                          "Value": ins9
                      },
                      {
                          "Name": "TableInstanceId",
                          "Value": tbl9
                      }
                  ]
              },
              {
                  "Dimensions": [
                      {
                          "Name": "ClusterId",
                          "Value": ins10
                      },
                      {
                          "Name": "TableInstanceId",
                          "Value": tbl10
                      }
                  ]
              }
          ],
          "Namespace": "QCE/TCAPLUS",
          "MetricName": metric_list[i],
          "Period": 300,
          "StartTime": c_time2,
          "EndTime": c_time1      
      }
      req.from_json_string(json.dumps(params))

      resp = client.GetMonitorData(req)
      
      resp_data = resp.to_json_string()

      with open(f"{cwd}/writefile{metric_list[i]}.json", "w+") as f:
          jsondata = f.read()
          f.seek(0)
          f.write(resp_data)
          print(resp_data)
          f.truncate()

except TencentCloudSDKException as err: 
    print(err)


for i in range(len(metric_list)):
  with open(f"{cwd}/writefile{metric_list[i]}.json") as file:
       input_data = json.load(file)
       data1 = glom(input_data, ('DataPoints', [('Dimensions', ['Value'])]))
       data2 = glom(input_data, ('DataPoints', [('Values')]))

       if len(data1) > 1:
        inst_id1 = data1[0][0]
        table_id1 = data1[0][1]
        values_1 = data2[0][0]

        inst_id2 = data1[1][0]
        table_id2 = data1[1][1]
        values_2 = data2[1][0]

        inst_id3 = data1[2][0]
        table_id3 = data1[2][1]
        values_3 = data2[2][0]

        inst_id4 = data1[3][0]
        table_id4 = data1[3][1]
        values_4 = data2[3][0]

        inst_id5 = data1[4][0]
        table_id5 = data1[4][1]
        values_5 = data2[4][0]

        inst_id6 = data1[5][0]
        table_id6 = data1[5][1]
        values_6 = data2[5][0]

        inst_id7 = data1[6][0]
        table_id7 = data1[6][1]
        values_7 = data2[6][0]

        inst_id8 = data1[7][0]
        table_id8 = data1[7][1]
        values_8 = data2[7][0]

        inst_id9 = data1[8][0]
        table_id9 = data1[8][1]
        values_9 = data2[8][0]

        inst_id10 = data1[9][0]
        table_id10 = data1[9][1]
        values_10 = data2[9][0]

        metric_name = metric_list[i]

       elif len(data1) == 1 :
        inst_id1,inst_id2,inst_id3,inst_id4,inst_id5,inst_id6,inst_id7,inst_id8,inst_id9,inst_id10 = data1[0][0],data1[0][0],data1[0][0],data1[0][0],data1[0][0],data1[0][0],data1[0][0],data1[0][0],data1[0][0],data1[0][0]
        table_id1,table_id2,table_id3,table_id4,table_id5,table_id6,table_id7,table_id8,table_id9,table_id10 = data1[0][1],data1[0][1],data1[0][1],data1[0][1],data1[0][1],data1[0][1],data1[0][1],data1[0][1],data1[0][1],data1[0][1]
        values_1,values_2,values_3,values_4,values_5,values_6,values_7,values_8,values_9,values_10 = data2[0][0],data2[0][0],data2[0][0],data2[0][0],data2[0][0],data2[0][0],data2[0][0],data2[0][0],data2[0][0],data2[0][0]
        metric_name = metric_list[i]

       else:
        None

  payload = createMetricData(metric_name,inst_id1,table_id1,values_1,inst_id2,table_id2,values_2,inst_id3,table_id3,values_3,inst_id4,table_id4,values_4,inst_id5,table_id5,values_5,inst_id6,table_id6,values_6,inst_id7,table_id7,values_7,inst_id8,table_id8,values_8,inst_id9,table_id9,values_9,inst_id10,table_id10,values_10)
  header_data = {"content-type": "application/json"}
  r = requests.post(endpointurl,data=json.dumps(payload), headers=header_data)
  print( r )
