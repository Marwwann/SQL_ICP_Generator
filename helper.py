import requests
import re
import bs4
import CallFusion

def createSelectQuery(element_type_id):
    return f'''WITH VALUES_QUERY AS(
SELECT
PIVF.BASE_NAME,
X.INPUT_VALUE_ID,
X.SCREEN_ENTRY_VALUE
FROM
HRC_TXN_HEADER HTH,
XMLTABLE(
'TRANSACTION/BUSINESSDATA/AM/TXN/EO[@Name = ''oracle.apps.hcm.payrolls.elements.entries.protectedModel.entity.ElementEntryDEO'']/ElementEntryDEORow/CEO/EO[@Name = ''oracle.apps.hcm.payrolls.elements.entries.protectedModel.entity.ElementEntryValueDEO'']/ElementEntryValueDEORow'
PASSING HTH.XML_DATA_CACHE
COLUMNS
"INPUT_VALUE_ID" VARCHAR2(200) PATH 'InputValueId/DATA',
"SCREEN_ENTRY_VALUE" VARCHAR2(200) PATH 'ScreenEntryValue/DATA'
) X,
PAY_INPUT_VALUES_F PIVF
WHERE 1=1
AND PIVF.INPUT_VALUE_ID = X.INPUT_VALUE_ID
AND TRUNC(SYSDATE) BETWEEN PIVF.EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
AND HTH.TRANSACTION_ID = :transId
AND EXTRACTVALUE(HTH.XML_DATA_CACHE, 'TRANSACTION/BUSINESSDATA/AM/TXN/EO[@Name = ''oracle.apps.hcm.payrolls.elements.entries.protectedModel.entity.ElementEntryDEO'']/ElementEntryDEORow/ElementTypeId/DATA') = {element_type_id}
AND EXTRACTVALUE(HTH.XML_DATA_CACHE, 'TRANSACTION/COMPAREDATA/CompareVO/VariableCompOverviewVORow/@operationType') = 'create')
SELECT\n'''

def createFromQuery(df_create):
    base_name = df_create.iloc[0, 3]
    return f'''FROM DUAL
WHERE (SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.BASE_NAME = '{base_name}') IS NOT NULL'''

def deleteSelectQuery():
    return '''WITH ELEMENT_ENTRY_ID_QUERY AS
(
SELECT DISTINCT
X."ELEMENT_ENTRY_ID" AS ELEMENT_ENTRY_ID
FROM 
HRC_TXN_HEADER HTH,
XMLTABLE('TRANSACTION/BUSINESSDATA/AM/TXN/EO[@Name = ''oracle.apps.hcm.payrolls.elements.entries.protectedModel.entity.ElementEntryTEO'']/ElementEntryTEORow/ElementEntryId'
PASSING HTH.XML_DATA_CACHE
COLUMNS
"ELEMENT_ENTRY_ID"   VARCHAR2(200) PATH 'DATA'
) X
WHERE 1=1
AND HTH.TRANSACTION_ID =:transId
AND EXTRACTVALUE(HTH.XML_DATA_CACHE, 'TRANSACTION/COMPAREDATA/CompareVO/VariableCompOverviewVORow/@operationType') = 'delete'
),
VALUES_QUERY AS
(
SELECT PEEVF.INPUT_VALUE_ID, PEEVF.SCREEN_ENTRY_VALUE, PEEF.ELEMENT_TYPE_ID, PIVF.BASE_NAME
FROM 
PAY_ELEMENT_ENTRY_VALUES_F PEEVF,
PAY_ELEMENT_ENTRIES_F PEEF,
PAY_INPUT_VALUES_F PIVF,
ELEMENT_ENTRY_ID_QUERY EEIQ
WHERE 1=1
AND PEEVF.ELEMENT_ENTRY_ID = EEIQ.ELEMENT_ENTRY_ID
AND PEEVF.ELEMENT_ENTRY_ID = PEEF.ELEMENT_ENTRY_ID
AND PEEVF.INPUT_VALUE_ID = PIVF.INPUT_VALUE_ID
AND TRUNC(SYSDATE) BETWEEN PIVF.EFFECTIVE_START_DATE AND PIVF.EFFECTIVE_END_DATE
)
SELECT\n'''

def deleteFromQuery(element_type_id):
    return f'''
    FROM DUAL
WHERE (SELECT DISTINCT ELEMENT_TYPE_ID FROM VALUES_QUERY) = {element_type_id}'''


def updateSelectQuery(element_type_id):
    return f'''WITH UPDATE_VALUES AS
(
SELECT
X."SCREEN_ENTRY_VALUE",
X."INPUT_VALUE_ID", 
PIVF.BASE_NAME, 
EXTRACTVALUE(HTH.XML_DATA_CACHE, 'TRANSACTION/BUSINESSDATA/AM/TXN/EO[@Name=''oracle.apps.hcm.payrolls.elements.entries.protectedModel.entity.ElementEntryDEO'']/ElementEntryDEORow/BIPData/BIPElementEntryId') AS ELEMENT_ENTRY_ID
FROM HRC_TXN_HEADER HTH,
XMLTABLE(
'TRANSACTION/BUSINESSDATA/AM/TXN/EO[@Name=''oracle.apps.hcm.payrolls.elements.entries.protectedModel.entity.ElementEntryDEO'']/ElementEntryDEORow/CEO/EO[@Name=''oracle.apps.hcm.payrolls.elements.entries.protectedModel.entity.ElementEntryValueDEO'']/ElementEntryValueDEORow'
PASSING HTH.XML_DATA_CACHE
COLUMNS
"SCREEN_ENTRY_VALUE" VARCHAR2(200) PATH 'ScreenEntryValue/DATA',
"INPUT_VALUE_ID"     VARCHAR2(200) PATH 'BIPData/BIPInputValueId'
) X,
PAY_INPUT_VALUES_F PIVF
WHERE 1=1
AND HTH.TRANSACTION_ID = :transId
AND PIVF.INPUT_VALUE_ID = X."INPUT_VALUE_ID"
AND EXTRACTVALUE(HTH.XML_DATA_CACHE, 'TRANSACTION/BUSINESSDATA/AM/TXN/EO[@Name=''oracle.apps.hcm.payrolls.elements.entries.protectedModel.entity.ElementEntryDEO'']/ElementEntryDEORow/BIPData/BIPElementTypeId') = {element_type_id}
AND EXTRACTVALUE(HTH.XML_DATA_CACHE, 'TRANSACTION/COMPAREDATA/CompareVO/VariableCompOverviewVORow/@operationType') = 'compare'
AND TRUNC(SYSDATE) BETWEEN PIVF.EFFECTIVE_START_DATE AND PIVF.EFFECTIVE_END_DATE
),
MAIN_QUERY AS
(
SELECT
PEEVF.SCREEN_ENTRY_VALUE AS MAIN,
UV.SCREEN_ENTRY_VALUE AS UPDATED,
(SELECT BASE_NAME FROM PAY_INPUT_VALUES_F WHERE INPUT_VALUE_ID = PEEVF.INPUT_VALUE_ID) AS BASE_NAME
FROM 
PAY_ELEMENT_ENTRY_VALUES_F PEEVF
LEFT JOIN UPDATE_VALUES UV
ON UV.ELEMENT_ENTRY_ID = PEEVF.ELEMENT_ENTRY_ID
AND UV.INPUT_VALUE_ID = PEEVF.INPUT_VALUE_ID
WHERE PEEVF.ELEMENT_ENTRY_ID = (SELECT DISTINCT ELEMENT_ENTRY_ID FROM UPDATE_VALUES)
)
SELECT\n'''


def updateFromQuery(df_update):
    base_name = df_update.iloc[0, 3]
    return f'''
    FROM DUAL
WHERE (SELECT NVL(UPDATED,MAIN) FROM MAIN_QUERY WHERE BASE_NAME = '{base_name}')  IS NOT NULL'''


def getMainQuery(total_string):
    return f'''SELECT 
PETT.REPORTING_NAME, 
PETT.ELEMENT_TYPE_ID, 
PIVF.INPUT_VALUE_ID, 
PIVF.BASE_NAME,
PIVF.VALUE_SET_CODE, 
FVVS.VALIDATION_TYPE,
CASE WHEN (INSTR(BASE_NAME, 'Date')) != 0 THEN
'TO_CHAR(TO_TIMESTAMP_TZ((SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.BASE_NAME = \'''|| PIVF.BASE_NAME ||\''' ),''YYYY-MM-DD HH24:MI:SS.FFTZHTZM''),''YYYY-MM-DD'')'
ELSE CASE WHEN (INSTR(BASE_NAME, 'Amount')) != 0 THEN 
'TRIM(TO_CHAR((SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.BASE_NAME = \'''|| PIVF.BASE_NAME ||\''' ),''999G999D99''))'
ELSE CASE WHEN FVVS.VALIDATION_TYPE = 'TABLE' THEN
'(SELECT VAL_COL FROM (' || CHR(10) ||
'SELECT' || CHR(10) || ID_COLUMN_NAME || ' AS ID_COL, ' || CHR(10) || VALUE_COLUMN_NAME || ' AS VAL_COL' || CHR(10)
|| 'FROM' || CHR(10) || APPLICATION_TABLE_NAME || CHR(10)
|| CASE WHEN ADDITIONAL_WHERE_CLAUSE IS NOT NULL THEN ADDITIONAL_WHERE_CLAUSE ELSE '' END || ') WHERE ID_COL = '
||
'(SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.BASE_NAME = \'''|| PIVF.BASE_NAME ||\''' ))'
ELSE
'(SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.BASE_NAME = \'''|| PIVF.BASE_NAME ||\''')'
END
END
END
|| ' AS ' || UPPER(REPLACE(BASE_NAME, ' ', '_')) || '_CREATE'                                                               AS MAIN_CREATE_QUERY,
CASE WHEN (INSTR(BASE_NAME, 'Date')) != 0 THEN
'TO_CHAR(TO_TIMESTAMP_TZ((SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''),''YYYY-MM-DD HH24:MI:SS.FFTZHTZM''),''YYYY-MM-DD'')'
ELSE CASE WHEN (INSTR(BASE_NAME, 'Amount')) != 0 THEN 
'TO_CHAR((SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''),''999G999D99'')'
ELSE CASE WHEN FVVS.VALIDATION_TYPE = 'TABLE' THEN
'(SELECT VAL_COL FROM (' || CHR(10) ||
'SELECT' || CHR(10) || ID_COLUMN_NAME || ' AS ID_COL, ' || CHR(10) || VALUE_COLUMN_NAME || ' AS VAL_COL' || CHR(10)
|| 'FROM' || CHR(10) || APPLICATION_TABLE_NAME || CHR(10)
|| CASE WHEN ADDITIONAL_WHERE_CLAUSE IS NOT NULL THEN ADDITIONAL_WHERE_CLAUSE ELSE '' END || ') WHERE ID_COL = '
||
'(SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''))'
ELSE
'(SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.BASE_NAME = \'''|| PIVF.BASE_NAME ||\''')'
END
END
END
|| ' AS ' || UPPER(REPLACE(BASE_NAME, ' ', '_')) || '_DELETE'                                                                AS MAIN_DELETE_QUERY,
CASE WHEN (INSTR(BASE_NAME, 'Date')) != 0 THEN
'TO_CHAR(TO_TIMESTAMP_TZ((SELECT NVL(UPDATED,MAIN) FROM MAIN_QUERY WHERE BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''),''YYYY-MM-DD HH24:MI:SS.FFTZHTZM''),''YYYY-MM-DD'')'
ELSE CASE WHEN (INSTR(BASE_NAME, 'Amount')) != 0 THEN 
'TO_CHAR((SELECT NVL(UPDATED,MAIN) FROM MAIN_QUERY WHERE BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''),''999G999D99'')'
ELSE CASE WHEN FVVS.VALIDATION_TYPE = 'TABLE' THEN
'(SELECT VAL_COL FROM (' || CHR(10) ||
'SELECT' || CHR(10) || ID_COLUMN_NAME || ' AS ID_COL, ' || CHR(10) || VALUE_COLUMN_NAME || ' AS VAL_COL' || CHR(10)
|| 'FROM' || CHR(10) || APPLICATION_TABLE_NAME || CHR(10)
|| CASE WHEN ADDITIONAL_WHERE_CLAUSE IS NOT NULL THEN ADDITIONAL_WHERE_CLAUSE ELSE '' END || ') WHERE ID_COL = '
||
'(SELECT NVL(UPDATED,MAIN) FROM MAIN_QUERY WHERE BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''))'
ELSE
'(SELECT NVL(UPDATED,MAIN) FROM MAIN_QUERY WHERE BASE_NAME = \'''|| PIVF.BASE_NAME ||\''')'
END
END
END
|| ' AS ' || UPPER(REPLACE(BASE_NAME, ' ', '_')) || '_UPDATE'                                                              AS MAIN_UPDATE_QUERY
FROM 
PAY_ELEMENT_TYPES_TL PETT,
PAY_INPUT_VALUES_F PIVF,
FND_VS_VALUE_SETS FVVS,
FND_FLEX_VALIDATION_TABLES FFVT
WHERE 1=1
AND PETT.ELEMENT_TYPE_ID = PIVF.ELEMENT_TYPE_ID
AND FVVS.VALUE_SET_CODE(+) = PIVF.VALUE_SET_CODE
AND FVVS.VALUE_SET_ID = FFVT.FLEX_VALUE_SET_ID (+)
AND PETT.LANGUAGE = 'US'
AND PIVF.USER_DISPLAY_FLAG = 'Y'
AND TRUNC(SYSDATE) BETWEEN PIVF.EFFECTIVE_START_DATE AND PIVF.EFFECTIVE_END_DATE
AND PETT.REPORTING_NAME IN ({total_string[:-2]})'''

def getPersonId():
    return '''(SELECT EXTRACTVALUE(XML_DATA_CACHE, 'TRANSACTION/TransCtx/compPersonId') AS PERSON_ID
FROM HRC_TXN_HEADER
WHERE TRANSACTION_ID = :transId)'''


old_query = f'''SELECT 
PETT.REPORTING_NAME, 
PETT.ELEMENT_TYPE_ID, 
PIVF.INPUT_VALUE_ID, 
PIVF.BASE_NAME,
PIVF.VALUE_SET_CODE, 
FVVS.VALIDATION_TYPE,
CASE WHEN (INSTR(BASE_NAME, 'Date')) != 0 THEN
'  TO_CHAR(TO_TIMESTAMP_TZ((
    SELECT extractvalue(value(elemententryvalues),''/ElementEntryValueDEORow/ScreenEntryValue/DATA'')
    FROM table(per_bipntf_utility.extractxmlsequenceforeo(txnheader.transaction_id, ''ElementEntryValueDEO'', 0)) elemententryvalues
    WHERE extractvalue(value(elemententryvalues),''/ElementEntryValueDEORow/InputValueId/DATA'') IN (
      SELECT input_value_id FROM pay_input_values_f
      WHERE sysdate BETWEEN effective_start_date AND effective_end_date
      AND base_name = \'''|| PIVF.BASE_NAME ||\'''
      AND element_type_id IN extractvalue(value(elemententries),''/ElementEntryDEORow/ElementTypeId/DATA'')
    )
  ),''YYYY-MM-DD HH24:MI:SS.FFTZHTZM''),''YYYY-MM-DD'')'
ELSE CASE WHEN (INSTR(BASE_NAME, 'Amount')) != 0 THEN 
'
TRIM(TO_CHAR((
    SELECT extractvalue(value(elemententryvalues),''/ElementEntryValueDEORow/ScreenEntryValue/DATA'')
    FROM table(per_bipntf_utility.extractxmlsequenceforeo(txnheader.transaction_id, ''ElementEntryValueDEO'', 0)) elemententryvalues
    WHERE extractvalue(value(elemententryvalues),''/ElementEntryValueDEORow/InputValueId/DATA'') IN (
      SELECT input_value_id FROM pay_input_values_f
      WHERE sysdate BETWEEN effective_start_date AND effective_end_date
      AND base_name = \'''|| PIVF.BASE_NAME ||\'''
      AND element_type_id IN extractvalue(value(elemententries),''/ElementEntryDEORow/ElementTypeId/DATA'')
    )
  ),''999G999D99''))
'
ELSE CASE WHEN FVVS.VALIDATION_TYPE = 'TABLE' THEN
'(SELECT VAL_COL FROM (' || CHR(10) ||
'SELECT' || CHR(10) || ID_COLUMN_NAME || ' AS ID_COL, ' || CHR(10) || VALUE_COLUMN_NAME || ' AS VAL_COL' || CHR(10)
|| 'FROM' || CHR(10) || APPLICATION_TABLE_NAME || CHR(10)
|| CASE WHEN ADDITIONAL_WHERE_CLAUSE IS NOT NULL THEN ADDITIONAL_WHERE_CLAUSE ELSE '' END || ') WHERE ID_COL = '
||
'
(
    SELECT extractvalue(value(elemententryvalues),''/ElementEntryValueDEORow/ScreenEntryValue/DATA'')
    FROM table(per_bipntf_utility.extractxmlsequenceforeo(txnheader.transaction_id, ''ElementEntryValueDEO'', 0)) elemententryvalues
    WHERE extractvalue(value(elemententryvalues),''/ElementEntryValueDEORow/InputValueId/DATA'') IN (
      SELECT input_value_id FROM pay_input_values_f
      WHERE sysdate BETWEEN effective_start_date AND effective_end_date
      AND base_name = \'''|| PIVF.BASE_NAME ||\'''
      AND element_type_id IN extractvalue(value(elemententries),''/ElementEntryDEORow/ElementTypeId/DATA'')
    )
  ))
'
ELSE
'
(
    SELECT extractvalue(value(elemententryvalues),''/ElementEntryValueDEORow/ScreenEntryValue/DATA'')
    FROM table(per_bipntf_utility.extractxmlsequenceforeo(txnheader.transaction_id, ''ElementEntryValueDEO'', 0)) elemententryvalues
    WHERE extractvalue(value(elemententryvalues),''/ElementEntryValueDEORow/InputValueId/DATA'') IN (
      SELECT input_value_id FROM pay_input_values_f
      WHERE sysdate BETWEEN effective_start_date AND effective_end_date
      AND base_name = \'''|| PIVF.BASE_NAME ||\'''
      AND element_type_id IN extractvalue(value(elemententries),''/ElementEntryDEORow/ElementTypeId/DATA'')
    )
  )
'
END
END
END
|| ' AS ' || UPPER(REPLACE(BASE_NAME, ' ', '_')) || '_CREATE'
AS MAIN_CREATE_QUERY,
CASE WHEN (INSTR(BASE_NAME, 'Date')) != 0 THEN
'  TO_CHAR(TO_TIMESTAMP_TZ((
SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.INPUT_VALUE_ID = 
(
SELECT INPUT_VALUE_ID 
FROM PAY_INPUT_VALUES_F 
WHERE 1=1
AND BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''
AND ELEMENT_TYPE_ID = (SELECT DISTINCT ELEMENT_TYPE_ID FROM VALUES_QUERY)
AND TRUNC(SYSDATE) BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
)
),''YYYY-MM-DD HH24:MI:SS.FFTZHTZM''),''YYYY-MM-DD'')'
ELSE CASE WHEN (INSTR(BASE_NAME, 'Amount')) != 0 THEN 
'
TO_CHAR((
SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.INPUT_VALUE_ID = 
(
SELECT INPUT_VALUE_ID 
FROM PAY_INPUT_VALUES_F 
WHERE 1=1
AND BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''
AND ELEMENT_TYPE_ID = (SELECT DISTINCT ELEMENT_TYPE_ID FROM VALUES_QUERY)
AND TRUNC(SYSDATE) BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
)
),''999G999D99'')
'
ELSE CASE WHEN FVVS.VALIDATION_TYPE = 'TABLE' THEN
'(SELECT VAL_COL FROM (' || CHR(10) ||
'SELECT' || CHR(10) || ID_COLUMN_NAME || ' AS ID_COL, ' || CHR(10) || VALUE_COLUMN_NAME || ' AS VAL_COL' || CHR(10)
|| 'FROM' || CHR(10) || APPLICATION_TABLE_NAME || CHR(10)
|| CASE WHEN ADDITIONAL_WHERE_CLAUSE IS NOT NULL THEN ADDITIONAL_WHERE_CLAUSE ELSE '' END || ') WHERE ID_COL = '
||
'
(
SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.INPUT_VALUE_ID = 
(
SELECT INPUT_VALUE_ID 
FROM PAY_INPUT_VALUES_F 
WHERE 1=1
AND BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''
AND ELEMENT_TYPE_ID = (SELECT DISTINCT ELEMENT_TYPE_ID FROM VALUES_QUERY)
AND TRUNC(SYSDATE) BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
)
))
'
ELSE
'
(
SELECT VQ.SCREEN_ENTRY_VALUE FROM VALUES_QUERY VQ WHERE VQ.INPUT_VALUE_ID = 
(
SELECT INPUT_VALUE_ID 
FROM PAY_INPUT_VALUES_F 
WHERE 1=1
AND BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''
AND ELEMENT_TYPE_ID = (SELECT DISTINCT ELEMENT_TYPE_ID FROM VALUES_QUERY)
AND TRUNC(SYSDATE) BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
)
)
'
END
END
END
|| ' AS ' || UPPER(REPLACE(BASE_NAME, ' ', '_')) || '_DELETE'
AS MAIN_DELETE_QUERY,
CASE WHEN (INSTR(BASE_NAME, 'Date')) != 0 THEN
'  TO_CHAR(TO_TIMESTAMP_TZ((SELECT NVL(UPDATED,MAIN) FROM MAIN_QUERY WHERE BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''),''YYYY-MM-DD HH24:MI:SS.FFTZHTZM''),''YYYY-MM-DD'')'
ELSE CASE WHEN (INSTR(BASE_NAME, 'Amount')) != 0 THEN 
'
TO_CHAR((SELECT NVL(UPDATED,MAIN) FROM MAIN_QUERY WHERE BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''),''999G999D99'')
'
ELSE CASE WHEN FVVS.VALIDATION_TYPE = 'TABLE' THEN
'(SELECT VAL_COL FROM (' || CHR(10) ||
'SELECT' || CHR(10) || ID_COLUMN_NAME || ' AS ID_COL, ' || CHR(10) || VALUE_COLUMN_NAME || ' AS VAL_COL' || CHR(10)
|| 'FROM' || CHR(10) || APPLICATION_TABLE_NAME || CHR(10)
|| CASE WHEN ADDITIONAL_WHERE_CLAUSE IS NOT NULL THEN ADDITIONAL_WHERE_CLAUSE ELSE '' END || ') WHERE ID_COL = '
||
'
(SELECT NVL(UPDATED,MAIN) FROM MAIN_QUERY WHERE BASE_NAME = \'''|| PIVF.BASE_NAME ||\'''))
'
ELSE
'
(SELECT NVL(UPDATED,MAIN) FROM MAIN_QUERY WHERE BASE_NAME = \'''|| PIVF.BASE_NAME ||\''')
'
END
END
END
|| ' AS ' || UPPER(REPLACE(BASE_NAME, ' ', '_')) || '_UPDATE'
AS MAIN_UPDATE_QUERY
FROM 
PAY_ELEMENT_TYPES_TL PETT,
PAY_INPUT_VALUES_F PIVF,
FND_VS_VALUE_SETS FVVS,
FND_FLEX_VALIDATION_TABLES FFVT
WHERE 1=1
AND PETT.ELEMENT_TYPE_ID = PIVF.ELEMENT_TYPE_ID
AND FVVS.VALUE_SET_CODE(+) = PIVF.VALUE_SET_CODE
AND FVVS.VALUE_SET_ID = FFVT.FLEX_VALUE_SET_ID (+)
AND PETT.LANGUAGE = 'US'
AND PIVF.USER_DISPLAY_FLAG = 'Y'
AND TRUNC(SYSDATE) BETWEEN PIVF.EFFECTIVE_START_DATE AND PIVF.EFFECTIVE_END_DATE'''


def validateAuthentication(url, username, password):
    url = url + ":443/xmlpserver/services/v2/SecurityService"
    regex = r"^https?:\/\/[\w\.-]+:?\d*\/?\w*\/*\??.*$"
    if re.match(regex, url) is None:
        return "The URL is not valid"

    payload = f"""
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                      xmlns:v2="http://xmlns.oracle.com/oxp/service/v2">
      <soapenv:Header/>
      <soapenv:Body>
        <v2:validateLogin>
          <v2:userID>{username}</v2:userID>
          <v2:password>{password}</v2:password>
        </v2:validateLogin>
      </soapenv:Body>
    </soapenv:Envelope>
    """

    headers = {'Content-Type': 'text/xml;charset=utf-8'}
    response = requests.post(url, data=payload.encode('utf-8'), headers=headers)

    if response.status_code.__str__()[0] == '5':
        return "The Server is not responding, please try again later."

    if response.status_code.__str__()[0] == '2':
        soup = bs4.BeautifulSoup(response.text.encode('utf8'), "xml")
        tag_value = soup.find('validateLoginReturn').string
        if tag_value == 'true':
            return ''
        else:
            return "Wrong username/password"

def getElementTypes(instanceURI, uname, pw):
    sql_query = """SELECT REPORTING_NAME, ELEMENT_TYPE_ID FROM PAY_ELEMENT_TYPES_TL WHERE LANGUAGE = 'US' ORDER BY REPORTING_NAME"""
    result_df = CallFusion.getResult(instanceURI, uname, pw, sql_query)
    return result_df.to_records(index=False)