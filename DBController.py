"""
Created on 2013-5-8
@author: Bobi Pu, bobi.pu@usc.edu
"""

from pymongo import MongoClient
from textUtils import *
from setting import *

class DBController(object):
	def __init__(self):
		try:
			self.client = MongoClient()
			# self.db = self.client.OriginalCorpus
			self.db = self.client.ChemCorpus
		except Exception as e:
			print e
			exit()

	def close(self):
		self.client.end_request()

	def dropSentence(self):
		self.db.sentence.drop()

	def insertArticle(self, articleDict):
		self.db.article.insert(articleDict)

	def insertSentence(self, sentenceDict):
		newId = self.db.sentence.count()
		sentenceDict['id'] = newId
		self.db.sentence.insert(sentenceDict)

	def getAllSentence(self, limit=0):
		return self.db.sentence.find(timeout=False).limit(limit)

	def getAllArticle(self, limit=0):
		return self.db.article.find(timeout=False).limit(limit)

	def getALLArticleIdWithPfm(self):
		articleIdList = [sentence['articleId'] for sentence in self.db.sentence.find()]
		return set(articleIdList)

	def getAllPfmSentenceWithAtrb(self):
		return self.db.sentence.find({'$or' : [{'ex' : {'$exists' : True, '$ne' : []}}, {'in' : {'$exists' : True, '$ne' : []}}]}, timeout=False)

	def getAllSentenceWithoutAtrb(self):
		return self.db.sentence.find({'$and' : [{'ex' : {'$exists' : False}}, {'in' : {'$exists' : False}}]}, timeout=False)

	def getAllAnnotatedSentenceWithType(self, atrbType):
		return self.db.AnnotatedSentence.find({'atrb' : atrbType}, timeout=False)

	def getAllPfmNegSentence(self):
		return self.db.sentence.find({'neg' : {'$exists' : True, '$ne' : []}}, timeout=False)

	def getAllSentenceWithArticleId(self, articleId):
		return self.db.sentence.find({'articleId' : articleId}, timeout=False)

	def getAllSentenceForOneType(self, wordType, limit=0):
		key = getKeyFromWordType(wordType)
		return self.db.sentence.find({key : {'$exists' : True, '$ne' : []}}, timeout=False).limit(limit)

	def getAllEngager(self):
		return self.db.engager.find(timeout=False)

	def getAllCompany(self):
		return self.db.company.find(timeout=False)

	def getAllSentenceWithWord(self, word):
		word = wrapWord(word)
		return self.db.sentence.find({'content' : {'$regex' : word}}, timeout=False)

	def getAllPfmNegAtrbSentenceWithString(self, string):
		return self.db.sentence.find({'$and' : [{'$or' : [{'ex' : {'$exists' : True, '$ne' : []}}, {'in' : {'$exists' : True, '$ne' : []}}]}, {'content' : {'$regex' : string}}]}, timeout=False)

	def getAllSentenceWithoutCiteWord(self):
		return self.db.sentence.find({'$or' : [{'cite' : {'$exists' : False}}, {'cite' : {'$exists' : True, '$size' : 0}}]}, timeout=False)

	def getAllArticleByCompanyCode(self, code):
		return self.db.article.find({'filePath' : {'$regex' : code}}, timeout=False)

	def getEngagerByName(self, name):
		return self.db.engager.find_one({'name' : name})

	def getCompanyByName(self, name):
		return self.db.company.find_one({'name' : name})

	def updateCompanyCEO(self, companyId, CEODict):
		self.db.company.update({'id' : companyId}, {'$set' : {'CEO' : CEODict}})

	def updateSentenceEngager(self, sentenceId, engagerIdList):
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'engager' : engagerIdList}})

	def updateSentenceCompany(self, sentenceId, companyIdList):
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'company' : companyIdList}})

	def updateSentenceCiteWord(self, sentenceId, citeWordList):
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'cite' : citeWordList}})

	def updateCiteDistance(self, sentenceId, isCiteCEO, isCiteAnalyst, isCiteCompany):
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'citeCEO' : isCiteCEO, 'citeAnalyst' : isCiteAnalyst, 'citeCompany' : isCiteCompany}})

	def updatePfmDistance(self, sentenceId, isPfmPos, isPfmNeg):
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'pfmPos' : isPfmPos, 'pfmNeg' : isPfmNeg}})

	def getArticleById(self, articleId):
		return self.db.article.find_one({'id' : articleId})

	def getCompanyById(self, companyId):
		return self.db.company.find_one({'id' : companyId})

	def getCompanyByCode(self, code):
		return self.db.company.find_one({'code' : code})

	def getPfmSentenceCount(self, articleId):
		return self.db.sentence.find({'articleId' : articleId}).count()

	def updateArticlePfmSentenceCount(self, articleId, count):
		isPfmRelated = True if count != 0 else False
		self.db.article.update({'id' : articleId}, {'$set' : {'pfmSentenceCount' : count, 'pfmRelated' : isPfmRelated}})

	def countPfmSentenceInArticle(self):
		articles = self.db.getAllArticle()
		for article in articles:
			count = self.db.getPfmSentenceCount(article['id'])
			self.db.updateArticlePfmSentenceCount(article['id'], count)

	def updateSentenceCluster(self, sentenceId, clusterNum):
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'clusterSentence' : clusterNum}})

	def getSentenceInCluster(self, clusterSentence):
		return self.db.sentence.find({'clusterSentence': {'$exists' : True, '$all' : [clusterSentence]}})

	def getSentenceCount(self):
		return self.db.sentence.count()

	def getSentenceInRange(self, startId, endId):
		if startId < 1 or endId > self.getSentenceCount():
			raise Exception('invalid sentence id range')
		return self.db.sentence.find({'id' : {'$gte' : startId, '$lte' : endId}})

	def buildUnigramTable(self):
		table, count = {}, 0
		sentences = self.getAllSentence()
		for sentence in sentences:
			words = getProcessedWordList(sentence)
			for word in words:
				if word not in table:
					table[word] = count
					count += 1
		self.db.unigramTable.insert(table)

	def getUnigramTable(self):
		return self.db.unigramTable.find_one()

	def getReverseUnigramTable(self):
		table = self.getUnigramTable()
		return dict(zip(table.values(), table.keys()))

	def insertAnnotatedSentence(self, sentenceDict):
		newId = self.db.AnnotatedSentence.count()
		sentenceDict['id'] = newId
		self.db.annotatedSentence.insert(sentenceDict)

	def updateSentenceAtrb(self, sentenceId, exWordList, inWordList):
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'ex' : exWordList, 'in' : inWordList}})

	def isSentenceDuplicate(self, string):
		if self.db.sentence.find_one({'content' : string}) is not None:
			return True
		else:
			return False

	def isArticleDuplicate(self, string):
		if self.db.article.find_one({'tailParagraph' : string}) is not None:
			return True
		else:
			return False

	def getParsedArticleIdDict(self):
		articleIdDitc = {}
		sentences = self.getAllSentence()
		for sentence in sentences:
			articleId = sentence['articleId']
			if articleId not in articleIdDitc:
				articleIdDitc[articleId] = True
		return articleIdDitc

	def insertEngager(self, engagerDict):
		newId = self.db.engager.count()
		engagerDict['id'] = newId
		self.db.engager.insert(engagerDict)

	def insertCompany(self, companyDict):
		if 'id' not in companyDict:
			newId = self.db.company.count()
			companyDict['id'] = newId
		self.db.company.insert(companyDict)

	def getEngagerById(self, engagerId):
		return self.db.engager.find_one({'id' : engagerId})

	def updateSentencePfm(self, sentenceId, pfmWordList, posWordList, negWordList):
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'pfm' : pfmWordList, 'pos' : posWordList, 'neg' : negWordList}})

	def getAllEngagerByType(self, engagerType):
		return list(self.db.engager.find({'type' : engagerType}))

	def getEngagerIdListByType(self, engagerType):
		engagers = self.getAllEngagerByType(engagerType)
		return [engager['id'] for engager in engagers]

	def getAllNoNameEngager(self):
		return self.db.engager.find({'gender' : {'$exists' : False}})

	def getAllEngagerByCompanyId(self, companyId):
		company = self.getCompanyById(companyId)
		CEOIdList =  list(set(company['CEO'].itervalues()))
		engagers = [self.getEngagerById(CEOId) for CEOId in CEOIdList]
		engagers.extend(self.getAllNoNameEngager())
		return engagers