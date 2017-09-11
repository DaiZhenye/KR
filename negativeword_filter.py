import pymysql
import pandas as pd

class NegativeWordFilter(object):
	def __init__(self):
		#********************读取否定词库******************************#
		self.__conn = pymysql.connect(host='127.0.0.1',user='root',passwd='',db='mkt_sem',charset='utf8')
		self.__cur = self.__conn.cursor()
		self.__sql1 = "select id,negativeword from negativeWords;"
		self.__negativeWordsDf = pd.read_sql(self.__sql1,conn)
		self.__cur.close()
		self.__conn.close()
	
	#********************过滤掉包含否定关键词的搜索词******************************#
	def __isnegativeword(self,query):
		for negativeword in self.__negativeWordsDf['negativeword']:
			if query.find(str(negativeword)) != -1:
				return negativeword
		else:
			return
	#********************过滤掉包含否定关键词的搜索词******************************#""

	def filtnegativeword(self,objs,del_f=False):
		objs["短语否定"] = objs['搜索词'].apply(self.__isnegativeword)
		if del_f:
			objs = objs[objs["短语否定"].isnull()]
		return objs