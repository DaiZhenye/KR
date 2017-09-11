import os
import numpy as np
import pandas as pd
import pymysql
import re

from c1_filter import C1Filter


os.chdir(r"D:\词库\天津")

#********************读取账户关键词******************************#
df1 = pd.read_csv('c1.csv',encoding='gbk')
df2 = pd.read_csv('c2.csv',encoding='gbk')
df = pd.concat([df1,df2],axis=0,ignore_index=True)
df['添加'] = '已添加'
col = ['关键词名称','添加']
df = df.reindex(columns=col)
df.drop_duplicates(subset='关键词名称',keep='first',inplace=True)

# ********************读取搜索词报告******************************#
FileNameList = []
pat = re.compile(r'sousuoci_.*\.csv')
for filename in os.listdir():
	if re.match(pat,filename):
		FileNameList.append(filename)
sou1 = pd.read_csv(FileNameList[0],encoding='gbk')
sou2 = pd.read_csv(FileNameList[1],encoding='gbk')
sou = pd.concat([sou1,sou2],axis=0,ignore_index=True)
co12 = ['推广计划','推广单元','关键词','搜索词','展现','点击','消费']
sou = sou.reindex(columns=co12)

#********************根据账户关键词去重复******************************#
tt = sou.merge(df,how='left',left_on='搜索词',right_on='关键词名称',copy=False)  #参数:copy什么意思
tt = tt[tt['添加'] != '已添加']
tt.drop(['关键词名称','添加'],axis=1,inplace=True)
tt = tt[tt['点击'] > 1]    #筛选出点击量大于1的搜索词

#********************读取否定词库******************************#
conn = pymysql.connect(host='127.0.0.1',user='root',passwd='',db='mkt_sem',charset='utf8')
cur = conn.cursor()
sql2 = "select id,exactnegativeword from exactnegativewords;"
exactnegativeWordsDf = pd.read_sql(sql2,conn)
cur.close()
conn.close()

#********************筛选出包含C1标签的搜索词******************************#""
c1f = C1Filter()
tt= c1f.filtc1(tt,del_c2=Ture)

#********************过滤掉包含否定关键词的搜索词******************************#
nf = NegativeWordFilter()
tt= nf.filtnegativeword(tt,del_f=False)

#********************过滤掉包含精确否定词的搜索词******************************#
tt = pd.merge(tt,exactnegativeWordsDf,how='left',left_on='搜索词',right_on='exactnegativeword')
tt.drop('id',axis=1,inplace=True)

#导出结果
tt.to_csv('tt.csv',encoding='gbk',index=False)

'''
	待完善：1、
			2、地域词
'''


