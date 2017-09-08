import os
import numpy as np
import pandas as pd
import pymysql
import re

os.chdir(r"D:\词库\北京")
os.getcwd()
os.listdir()
#获取搜索词报告filename
FileNameList = []
pat = re.compile(r'sousuoci_.*\.csv')
for filename in os.listdir():
	if re.match(pat,filename):
		FileNameList.append(filename)
#读取词库
df1 = pd.read_csv('c1.csv',encoding='gbk')
df2 = pd.read_csv('c2.csv',encoding='gbk')
df = pd.concat([df1,df2],axis=0,ignore_index=True)
df['添加'] = '已添加'
col = ['关键词名称','添加']
df = df.reindex(columns=col)
df.drop_duplicates(subset='关键词名称',keep='first',inplace=True)
# 读取搜索词报告
sou1 = pd.read_csv(FileNameList[0],encoding='gbk')
sou2 = pd.read_csv(FileNameList[1],encoding='gbk')
sou = pd.concat([sou1,sou2],axis=0,ignore_index=True)
co12 = ['推广计划','推广单元','关键词','搜索词','展现','点击','消费']
sou = sou.reindex(columns=co12)
#根据词库去重复
tt = sou.merge(df,how='left',left_on='搜索词',right_on='关键词名称',copy=True)
tt = tt[tt['添加'] != '已添加']
tt.drop(['关键词名称','添加'],axis=1,inplace=True)
tt = tt[tt['点击'] > 1]
#读取否定词库
conn = pymysql.connect(host='127.0.0.1',user='root',passwd='',db='mkt_sem',charset='utf8')
cur = conn.cursor()
sql1 = "select id,negativeword from negativeWords;"
sql2 = "select id,exactnegativeword from exactnegativewords;"
sql3 = "select id,label from c1label;"
negativeWordsDf = pd.read_sql(sql1,conn)
exactnegativeWordsDf = pd.read_sql(sql2,conn)
c1labelDf = pd.read_sql(sql3,conn)
cur.close()
conn.close()
#筛选出包含C1标签的搜索词

def isc1keyword(query):
    for c1label in c1labelDf['label']:
        if query.find(str(c1label)) != -1:
            return c1label
    else:
        return
tt['是否C1'] = tt['搜索词'].apply(isc1keyword)
tt = tt[tt['是否C1'].notnull()]

#过滤掉包含否定关键词的搜索词
def isnegativeword(query):
    for negativeword in negativeWordsDf['negativeword']:
        if query.find(str(negativeword)) != -1:
            return negativeword
    else:
        return
tt["短语否定"] = tt['搜索词'].apply(isnegativeword)
#过滤掉包含精确否定词的搜索词
tt = pd.merge(tt,exactnegativeWordsDf,how='left',left_on='搜索词',right_on='exactnegativeword')
tt.drop('id',axis=1,inplace=True)
tt.to_csv('tt.csv',encoding='gbk',index=False)

#导出结果


