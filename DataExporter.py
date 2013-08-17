"""
Created on 2013-5-10
@author: Bobi Pu, bobi.pu@usc.edu
"""

from DBController import DBController
import csv
import os
from TextUtils import *
from Setting import *
from threading import Thread, activeCount
from Queue import Queue
from sklearn.feature_extraction.text import CountVectorizer
from numpy import sum
from FileUtils import getAtrbTypeKeyFromFolderName


class DataExporter(object):
	def __init__(self):
		self.db = DBController()
		if not os.path.exists('export/'):
			os.makedirs('export/')

	#sentence collection is all the sentence
	def exportSentenceAnalysis(self):
		with open('export/sentence.csv', 'wb') as f:
			writer = csv.writer(f)
			sentences = self.db.getAllSentence()
			articleDict = {}
			attributeList = ['id', 'cotic', 'coname', 'filePath', 'accessionNo', 'content', 'coname','ceoname', 'cite',
			                 'co_c', 'ceo_c', 'analyst_c', 'pfm', 'pfm_words', 'pos', 'pos_words', 'neg', 'neg_words',
			                 'internal', 'int_words', 'external', 'ext_words',
			                 'quote_sen', 'analyst']
			writer.writerow(attributeList)
			for i, sentence in enumerate(sentences):
				try:
					print(i)
					if sentence['articleId'] not in articleDict:
						articleDict[sentence['articleId']] = self.db.getArticleById(sentence['articleId'])
					article = articleDict[sentence['articleId']]
					articleCompanyCode = article['filePath'].split('/')[-2]
					articleCompany = self.db.getCompanyByCode(articleCompanyCode)
					articleCompanyName = articleCompanyCode if articleCompany is None else articleCompany['name']
					sentenceCompanyList = [self.db.getCompanyById(companyId) for companyId in sentence['company']]
					sentenceCompanyNameString = ','.join([company['shortName'] for company in sentenceCompanyList])
					sentenceEngagerList = [self.db.getEngagerById(engagerId) for engagerId in sentence['engager']]
					CEOList = filter(lambda engager : engager['type'] == ENGAGER_CEO, sentenceEngagerList)
					analystList =  filter(lambda engager : engager['type'] == ENGAGER_ANALYST, sentenceEngagerList)
					CEONameString = ','.join([CEO['lastName'] for CEO in CEOList])
					citeWordString = ','.join(sentence['cite'])
					citeCompany, citeCEO, citeAnalyst = int(sentence['citeCompany']), int(sentence['citeCEO']), int(sentence['citeAnalyst'])
					pfmWordString = ','.join(sentence['pfm'])
					posWordString = ','.join(sentence['pos'])
					negWordString = ','.join(sentence['neg'])
					inWordString = ','.join(sentence['in'])
					exWordString = ','.join(sentence['ex'])
					quoteString = getQuotedString(sentence['content'])
					analystSurroundString = getStringSurroundWordInDistance(sentence['content'], 'analyst', ANALYST_SURROUND_DISTANCE)
					lineList = [sentence['_id'], articleCompanyCode, articleCompanyName, article['filePath'], article['_id'], sentence['content'].encode('utf-8'),
					            sentenceCompanyNameString, CEONameString, citeWordString, citeCompany, citeCEO, citeAnalyst,
					            len(sentence['pfm']), pfmWordString, len(sentence['pos']), posWordString, len(sentence['neg']), negWordString,
					            len(sentence['in']), inWordString, len(sentence['ex']), exWordString,
					            quoteString, analystSurroundString]

					writer.writerow(lineList)
				except Exception as e:
					print(e)

	def exportArticleAnalysis(self):
		with open('export/article.csv', 'wb') as f:
			writer = csv.writer(f)
			articleList = list(self.db.getAllArticle())
			attributeList = ['cotic', 'coname', 'filePath', 'accessNo', 'date', 'source', 'author',
			                 'coname1', 'coname2', 'coname3', 'coname4', 'coname5',
			                 'subjectCode1', 'subjectCode2', 'subjectCode3', 'subjectCode4', 'subjectCode5']
			writer.writerow(attributeList)
			for i, article in enumerate(articleList):
				try:
					print(i)
					articleCompanyCode = article['filePath'].split('/')[-2]
					articleCompany = self.db.getCompanyByCode(articleCompanyCode)
					articleCompanyName = articleCompanyCode if articleCompany is None else articleCompany['name']
					companyCodeList = [''] * ARTICLE_EXPORT_CODE_SIZE
					subjectCodeList = [''] * ARTICLE_EXPORT_CODE_SIZE
					if 'company' in article:
						for i, companyCode in enumerate(article['company']):
							if i >= ARTICLE_EXPORT_CODE_SIZE:
								break
							companyCodeList[i] = companyCode
					else:
						article['company'] = [articleCompanyCode]
						companyCodeList = article['company']

					if 'newsSubject' in article:
						for i, subjectCode in enumerate(article['newsSubject']):
							if i >= ARTICLE_EXPORT_CODE_SIZE:
								break
							subjectCodeList[i] = subjectCode
					else:
						article['newsSubject'] = []
						subjectCodeList = article['newsSubject']

					self.db.saveArticle(article)

					lineList = [articleCompanyCode, articleCompanyName, article['filePath'], article['_id'], article['date'], article['sourceName'], article['byline']] + companyCodeList + subjectCodeList
					writer.writerow(lineList)
				except Exception as e:
					print(e)


if __name__ == '__main__':
	de = DataExporter()
	# de.exportSentenceAnalysis()
	de.exportArticleAnalysis()