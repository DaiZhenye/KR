import pymysql
import pandas as pd

class C1Filter(object):
	def __init__(self):
		#********************读取否定词库******************************#
		self.__conn = pymysql.connect(host='127.0.0.1',user='root',passwd='',db='mkt_sem',charset='utf8')
		self.__cur = self.__conn.cursor()
		self.__sql3 = "select id,label from c1label;"
		self.__c1labelDf = pd.read_sql(self.__sql3,self.__conn)
		self.__cur.close()
		self.__conn.close()
	
	#********************筛选出包含C1标签的搜索词******************************#""
	def __isc1keyword(self,query):
		for c1label in self.__c1labelDf['label']:
			if query.find(str(c1label)) != -1:
				return c1label
		else:
			return
			
	def filtc1(self,objs,del_c2=True):
		objs['是否C1'] = objs['搜索词'].apply(self.__isc1keyword)
		if del_c2:
			objs = objs[objs['是否C1'].notnull()]
		return objs