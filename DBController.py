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
			# self.db = self.client.test
			self._db = MongoClient().ChemCorpus
		except Exception as e:
			print e
			exit()

	def dropSentence(self):
		self._db.sentence.drop()

	def insertArticle(self, articleDict):
		self._db.article.insert(articleDict)

	def insertArticleInBatch(self, articleDictList):
		self._db.article.insert(articleDictList)

	def insertSentence(self, sentenceDict):
		newId = self._db.sentence.count()
		sentenceDict['_id'] = newId
		self._db.sentence.insert(sentenceDict)

	def insertAnnotatedSentence(self, sentenceDict):
		newId = self._db.AnnotatedSentence.count()
		sentenceDict['_id'] = newId
		self._db.annotatedSentence.insert(sentenceDict)

	def insertEngager(self, engagerDict):
		newId = self._db.engager.count()
		engagerDict['_id'] = newId
		self._db.engager.insert(engagerDict)

	def insertCompany(self, companyDict):
		if '_id' not in companyDict:
			newId = self._db.company.count()
			companyDict['_id'] = newId
		self._db.company.insert(companyDict)

	def getAllSentence(self, limit=0):
		return self._db.sentence.find(timeout=False).limit(limit)

	def getAllArticle(self, limit=0):
		return self._db.article.find(timeout=False).limit(limit)

	def getALLArticleIdWithPfm(self):
		articleIdList = [sentence['articleId'] for sentence in self._db.sentence.find()]
		return set(articleIdList)

	def getAllPfmSentenceWithAtrb(self):
		return self._db.sentence.find({'$or' : [{'ex' : {'$exists' : True, '$ne' : []}}, {'in' : {'$exists' : True, '$ne' : []}}]}, timeout=False)

	def getAllSentenceWithoutAtrb(self):
		return self._db.sentence.find({'$and' : [{'ex' : {'$exists' : False}}, {'in' : {'$exists' : False}}]}, timeout=False)

	def getAllAnnotatedSentenceWithType(self, atrbType):
		return self._db.AnnotatedSentence.find({'atrb' : atrbType}, timeout=False)

	def getAllPfmNegSentence(self):
		return self._db.sentence.find({'neg' : {'$exists' : True, '$ne' : []}}, timeout=False)

	def getAllSentenceWithArticleId(self, articleId):
		return self._db.sentence.find({'articleId' : articleId}, timeout=False)

	def getAllSentenceForOneType(self, wordType, limit=0):
		key = getKeyFromWordType(wordType)
		return self._db.sentence.find({key : {'$exists' : True, '$ne' : []}}, timeout=False).limit(limit)

	def getAllEngager(self):
		return self._db.engager.find(timeout=False)

	def getAllCompany(self):
		return self._db.company.find(timeout=False)

	def getAllSentenceWithWord(self, word):
		word = wrapWord(word)
		return self._db.sentence.find({'content' : {'$regex' : word}}, timeout=False)

	def getAllPfmNegAtrbSentenceWithString(self, string):
		return self._db.sentence.find({'$and' : [{'$or' : [{'ex' : {'$exists' : True, '$ne' : []}}, {'in' : {'$exists' : True, '$ne' : []}}]}, {'content' : {'$regex' : string}}]}, timeout=False)

	def getAllSentenceWithoutCiteWord(self):
		return self._db.sentence.find({'$or' : [{'cite' : {'$exists' : False}}, {'cite' : {'$exists' : True, '$size' : 0}}]}, timeout=False)

	def getAllArticleByCompanyCode(self, code):
		return self._db.article.find({'filePath' : {'$regex' : code}}, timeout=False)

	def getAllEngagerIdListByType(self, engagerType):
		engagers = self.getAllEngagerByType(engagerType)
		return [engager['_id'] for engager in engagers]

	def getAllEngagerByType(self, engagerType):
		return list(self._db.engager.find({'type' : engagerType}))

	def getAllNoNameEngager(self):
		return self._db.engager.find({'gender' : {'$exists' : False}})

	def getAllEngagerByCompanyId(self, companyId):
		company = self.getCompanyById(companyId)
		CEOIdList =  list(set(company['CEO'].itervalues()))
		engagers = [self.getEngagerById(CEOId) for CEOId in CEOIdList]
		engagers.extend(self.getAllNoNameEngager())
		return engagers

	def getEngagerByName(self, name):
		return self._db.engager.find_one({'name' : name})

	def getCompanyByName(self, name):
		return self._db.company.find_one({'name' : name})

	def updateCompanyCEO(self, companyId, CEODict):
		self._db.company.update({'_id' : companyId}, {'$set' : {'CEO' : CEODict}})

	def updateSentenceEngager(self, sentenceId, engagerIdList):
		self._db.sentence.update({'_id' : sentenceId}, {'$set' : {'engager' : engagerIdList}})

	def updateSentenceCompany(self, sentenceId, companyIdList):
		self._db.sentence.update({'_id' : sentenceId}, {'$set' : {'company' : companyIdList}})

	def updateSentenceCiteWord(self, sentenceId, citeWordList):
		self._db.sentence.update({'_id' : sentenceId}, {'$set' : {'cite' : citeWordList}})

	def updateCiteDistance(self, sentenceId, isCiteCEO, isCiteAnalyst, isCiteCompany):
		self._db.sentence.update({'_id' : sentenceId}, {'$set' : {'citeCEO' : isCiteCEO, 'citeAnalyst' : isCiteAnalyst, 'citeCompany' : isCiteCompany}})

	def updatePfmDistance(self, sentenceId, isPfmPos, isPfmNeg):
		self._db.sentence.update({'_id' : sentenceId}, {'$set' : {'pfmPos' : isPfmPos, 'pfmNeg' : isPfmNeg}})

	def getArticleById(self, articleId):
		return self._db.article.find_one({'_id' : articleId})

	def getCompanyById(self, companyId):
		return self._db.company.find_one({'_id' : companyId})

	def getCompanyByCode(self, code):
		return self._db.company.find_one({'code' : code})

	def updateSentenceCluster(self, sentenceId, clusterNum):
		self._db.sentence.update({'_id' : sentenceId}, {'$set' : {'clusterSentence' : clusterNum}})

	def getSentenceCount(self):
		return self._db.sentence.count()

	def getSentenceInRange(self, startId, endId):
		if startId < 1 or endId > self.getSentenceCount():
			raise Exception('invalid sentence id range')
		return self._db.sentence.find({'_id' : {'$gte' : startId, '$lte' : endId}})

	def getUnigramTable(self):
		return self._db.unigramTable.find_one()

	def updateSentenceAtrb(self, sentenceId, exWordList, inWordList):
		self._db.sentence.update({'_id' : sentenceId}, {'$set' : {'ex' : exWordList, 'in' : inWordList}})

	def isArticleDuplicate(self, string):
		if self._db.article.find_one({'tailParagraph' : string}) is not None:
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

	def getEngagerById(self, engagerId):
		return self._db.engager.find_one({'_id' : engagerId})

	def updateSentencePfm(self, sentenceId, pfmWordList, posWordList, negWordList):
		self._db.sentence.update({'_id' : sentenceId}, {'$set' : {'pfm' : pfmWordList, 'pos' : posWordList, 'neg' : negWordList}})

	def saveSentence(self, sentenceDict):
		self._db.sentence.save(sentenceDict)
