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
			self.db = self.client.TextAnalysis
		except Exception as e:
			print e
			exit()

	def close(self):
		self.client.end_request()

	def insertArticle(self, articleDict):
		self.db.article.insert(articleDict)

	def insertSentence(self, sentenceDict):
		newId = self.db.sentence.count() + 1
		sentenceDict['id'] = newId
		self.db.sentence.insert(sentenceDict)

	def getAllSentenceIter(self, limit=0):
		return self.db.sentence.find(timeout=False).limit(limit)

	def getAllArticleIter(self):
		return self.db.article.find(timeout=False)

	def getALLArticleIdWithPfm(self):
		articleIdList = [sentence['articleId'] for sentence in self.db.sentence.find()]
		return set(articleIdList)

	def getAllPfmNegSentenceIterWithAtrb(self):
		return self.db.sentence.find({'$or' : [{'ex' : {'$exists' : True, '$ne' : []}}, {'in' : {'$exists' : True, '$ne' : []}}]}, timeout=False)

	def getAllSentenceIterWithoutAtrb(self):
		return self.db.sentence.find({'$and' : [{'ex' : {'$exists' : False}}, {'in' : {'$exists' : False}}]}, timeout=False)

	def getAllAnnotatedSentenceIterWithType(self, atrbType):
		return self.db.AnnotatedSentence.find({'atrb' : atrbType}, timeout=False)

	def getAllPfmNegSentenceIter(self):
		return self.db.sentence.find({'neg' : {'$exists' : True, '$ne' : []}}, timeout=False)

	def getAllSentenceIterWithArticleId(self, articleId):
		return self.db.sentence.find({'articleId' : articleId}, timeout=False)

	def getAllSentenceIterForOneType(self, wordType, limit=0):
		key = getKeyFromWordType(wordType)
		return self.db.sentence.find({key : {'$exists' : True, '$ne' : []}}, timeout=False).limit(limit)

	def getAllEngager(self):
		return self.db.engager.find(timeout=False)

	def getAllCompany(self):
		return self.db.company.find(timeout=False)

	def getAllSentenceWithString(self, string):
		return self.db.sentence.find({'content' : {'$regex' : string}}, timeout=False)

	def getAllPfmNegAtrbSentenceWithString(self, string):
		return self.db.sentence.find({'$and' : [{'$or' : [{'ex' : {'$exists' : True, '$ne' : []}}, {'in' : {'$exists' : True, '$ne' : []}}]}, {'content' : {'$regex' : string}}]}, timeout=False)

	def updateSentenceEngager(self, sentenceId, engagerId):
		sentence = self.db.sentence.find_one({'id' : sentenceId})
		if 'engager' in sentence and engagerId not in sentence['engager']:
			sentence['engager'].append(engagerId)
			engagerList = sentence['engager']
		else:
			engagerList = [engagerId]
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'engager' : engagerList}})

	def updateSentenceCompany(self, sentenceId, companyId):
		sentence = self.db.sentence.find_one({'id' : sentenceId})
		if 'company' in sentence and companyId not in sentence['company']:
			sentence['company'].append(companyId)
			companyList = sentence['company']
		else:
			companyList = [companyId]
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'company' : companyList}})

	def updateSentenceCiteWord(self, sentenceId, word):
		sentence = self.db.sentence.find_one({'id' : sentenceId})
		key = getKeyFromWordType(CITE_WORD)
		if key in sentence and word not in sentence[key]:
			sentence[key].append(word)
			citeWordList = sentence[key]
		else:
			citeWordList = [word]
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'cite' : citeWordList}})


	def getArticleById(self, articleId):
		return self.db.article.find_one({'id' : articleId})

	def getPfmSentenceCount(self, articleId):
		return self.db.sentence.find({'articleId' : articleId}).count()

	def updateArticlePfmSentenceCount(self, articleId, count):
		isPfmRelated = True if count != 0 else False
		self.db.article.update({'id' : articleId}, {'$set' : {'pfmSentenceCount' : count, 'pfmRelated' : isPfmRelated}})

	def countPfmSentenceInArticle(self):
		articles = self.db.getAllArticleIter()
		for article in articles:
			count = self.db.getPfmSentenceCount(article['id'])
			self.db.updateArticlePfmSentenceCount(article['id'], count)

	def updateSentenceCluster(self, sentenceId, clusterNum):
		self.db.sentence.update({'id' : sentenceId}, {'$set' : {'clusterSentence' : clusterNum}})

	def getSentenceIterInCluster(self, clusterSentence):
		return self.db.sentence.find({'clusterSentence': {'$exists' : True, '$all' : [clusterSentence]}})

	def getSentenceCount(self):
		return self.db.sentence.count()

	def getSentenceIterInRange(self, startId, endId):
		if startId < 1 or endId > self.getSentenceCount():
			raise Exception('invalid sentence id range')
		return self.db.sentence.find({'id' : {'$gte' : startId, '$lte' : endId}})

	def buildUnigramTable(self):
		table, count = {}, 0
		sentences = self.getAllSentenceIter()
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
		sentences = self.getAllSentenceIter()
		for sentence in sentences:
			articleId = sentence['articleId']
			if articleId not in articleIdDitc:
				articleIdDitc[articleId] = True
		return articleIdDitc

	def getSentenceDate(self, articleId):
		return self.db.article.find_one({'id' : articleId})['date']

	def insertEngager(self, nameList, engagerType):
		for name in nameList:
			nameParts = name.split()
			if len(nameParts) >= 3:
				firstName, lastName = nameParts[0], nameParts[2]
			else:
				firstName, lastName = nameParts[0], nameParts[-1]
			newId = self.db.engager.count()
			self.db.engager.insert({'id': newId, 'fullName' : name, 'firstName': firstName, 'lastName': lastName, 'type' : engagerType})

	def insertCompany(self, nameList):
		for name in nameList:
			newId = self.db.company.count()
			shortName = cleanCompanyName(name)
			self.db.company.insert({'id' : newId, 'shortName' : shortName, 'fullName' : name})

	def getEngagerById(self, engagerId):
		return self.db.engager.find_one({'id' : engagerId})