import base64
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from io import StringIO

xdmData = f'''<?xml version = '1.0' encoding = 'utf-8'?>
<dataModel xmlns="http://xmlns.oracle.com/oxp/xmlp" version="2.0" xmlns:xdm="http://xmlns.oracle.com/oxp/xmlp" xmlns:xsd="http://wwww.w3.org/2001/XMLSchema" defaultDataSourceRef="AuditViewDB">
   <description>
      <![CDATA[undefined]]>
   </description>
   <dataProperties>
      <property name="include_parameters" value="false"/>
      <property name="include_null_Element" value="true"/>
      <property name="include_rowsettag" value="false"/>
      <property name="exclude_tags_for_lob" value="false"/>
      <property name="xml_tag_case" value="upper"/>
      <property name="sql_monitor_report_generated" value="false"/>
      <property name="optimize_query_executions" value="false"/>
   </dataProperties>
   <dataSets>
      <dataSet name="Marwan2" type="complex">
         <sql dataSourceRef="ApplicationDB_HCM">
            <![CDATA[SELECT 123 FROM DUAL]]>
         </sql>
      </dataSet>
   </dataSets>
   <output rootName="DATA_DS" uniqueRowName="false">
      <nodeList name="Marwan2"/>
   </output>
   <eventTriggers/>
   <lexicals/>
   <parameters>
   </parameters>
   <valueSets/>
   <bursting/>
   <validations>
      <validation>N</validation>
   </validations>
   <display>
      <layouts>
         <layout name="FST2" left="280px" top="349px"/>
         <layout name="DATA_DS" left="0px" top="349px"/>
      </layouts>
      <groupLinks/>
   </display>
</dataModel>
'''

instanceURL = "https://iaaqkf-test.fa.ocs.oraclecloud.com/"
fusionUserName = "mmetwaly@ejada.com"
fusionPassword = "WelcomeEjada456"
url = instanceURL + "/xmlpserver/services/v2/CatalogService"

encodedXDM = base64.b64encode(bytes(xdmData, encoding='utf8'))
payload = ("<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:v2=\"http://xmlns.oracle.com/oxp/service/v2\">\r\n    <soapenv:Header/>\r\n    <soapenv:Body>\r\n        <v2:updateObject>\r\n            <v2:objectAbsolutePath>/Custom/Human Capital Management/FusionSQLtoolTest1/FusionSQLToolDM.xdm</v2:objectAbsolutePath>\r\n            <v2:objectData>"
                   + str(encodedXDM.decode("utf-8")) + "</v2:objectData>\r\n            <v2:userID>" + fusionUserName + "</v2:userID>\r\n            <v2:password>" + fusionPassword + "</v2:password>\r\n        </v2:updateObject>\r\n    </soapenv:Body>\r\n</soapenv:Envelope>")
headers = {
            'Content-Type': 'text/xml;charset=UTF-8',
            'Host': instanceURL.replace('https://', ''),
            'SOAPAction': '""'
        }
response = requests.request("POST", url, headers=headers, data=payload)

print(response)