"""
Created on 2013-5-8
@author: Bobi Pu, bobi.pu@usc.edu
"""
import itertools

from pymongo import MongoClient
import re
from TextUtils import getPatternByKeywordSearchString

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

	def saveArticle(self, articleDict):
		self._db.article.save(articleDict)

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

	def getAllEngager(self):
		return self._db.engager.find(timeout=False)

	def getAllCompany(self):
		return self._db.company.find(timeout=False)

	def getAllBrokerageEffectiveNameList(self):
		nameList = []
		orgNameList = ['corp', 'inc', 'securities', 'research', 'bank', 'co', 'llc', 'group', 'l.l.c', 'ltd']
		orgNameListPatternString = r'|'.join([r'\b' + orgName + r'\b' for orgName in orgNameList])
		orgNameListPattern = re.compile(orgNameListPatternString, re.IGNORECASE)

		for brokerageDict in self._db.brokerage.find(timeout=False):
			name = brokerageDict['name']
			result = orgNameListPattern.search(name)
			if result is not None:
				i = result.regs[0][0] - 1
				if i != -1:
					while not name[i].isalpha():
						i -= 1
					name = name[:i + 1].strip()
				if len(name.split()) <= 1:
					name = brokerageDict['name']
			name = name.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.')
			nameList.append(name)
		return nameList

	def getAllSentenceWithWord(self, word):
		return self._db.sentence.find({'content' : re.compile(r'\b' + word + r'\b')}, timeout=False)

	def getAllSentenceWithoutCiteWord(self):
		return self._db.sentence.find({'$or' : [{'cite' : {'$exists' : False}}, {'cite' : {'$exists' : True, '$size' : 0}}]}, timeout=False)

	def getAllArticleByCompanyCode(self, code):
		code = re.sub(r'\.', r'\.', code)
		return self._db.article.find({'filePath' : re.compile(code)}, timeout=False)

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

	def getAllArticleBySearchString(self, searchString):
		if searchString is None:
			return []
		else:
			if searchString.find(',') != -1:
				#to boost processing speed, use iterative way
				keywordList = [keyword.strip() for keyword in searchString.split(',')]
				articleDict = {}
				for keywordTuple in itertools.permutations(keywordList):
					regexString = r'\b' + r'\b[^\.]*\b'.join(keywordTuple) + r'\b'
					pattern = re.compile(regexString, re.IGNORECASE)
					for article in self._db.article.find({'$or' : [{'byline' : pattern}, {'headline' : pattern}, {'leadParagraph' : pattern}, {'tailParagraph' : pattern}]}, timeout=False):
						articleDict[article['_id']] = article
				return articleDict.itervalues()
			elif searchString.find('|') != -1:
				#to boost processing speed, use iterative way
				articleDict = {}
				for keyword in searchString.split('|'):
					regexString = r'\b' + keyword.strip() + r'\b'
					pattern = re.compile(regexString, re.IGNORECASE)
					for article in self._db.article.find({'$or' : [{'byline' : pattern}, {'headline' : pattern}, {'leadParagraph' : pattern}, {'tailParagraph' : pattern}]}, timeout=False):
						articleDict[article['_id']] = article
				return articleDict.itervalues()
			else:
				pattern = getPatternByKeywordSearchString(searchString)
				return self._db.article.find({'$or' : [{'byline' : pattern}, {'headline' : pattern}, {'leadParagraph' : pattern}, {'tailParagraph' : pattern}]}, timeout=False)


	def getEngagerByName(self, name):
		return self._db.engager.find_one({'name' : name})

	def getCompanyByName(self, name):
		return self._db.company.find_one({'name' : name})

	def updateCompanyCEO(self, companyId, CEODict):
		self._db.company.update({'_id' : companyId}, {'$set' : {'CEO' : CEODict}})

	def getArticleById(self, articleId):
		return self._db.article.find_one({'_id' : articleId})

	def getCompanyById(self, companyId):
		return self._db.company.find_one({'_id' : companyId})

	def getCompanyByCode(self, code):
		code = re.sub(r'\.', r'\.', code)
		return self._db.company.find_one({'code' : re.compile(r'\b' + code + r'\b', re.IGNORECASE)})

	def updateSentenceCluster(self, sentenceId, clusterNum):
		self._db.sentence.update({'_id' : sentenceId}, {'$set' : {'clusterSentence' : clusterNum}})

	def getSentenceCount(self):
		return self._db.sentence.count()

	def getArticleCount(self):
		return self._db.article.count()

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

	def saveBrokerage(self, brokerageDict):
		self._db.brokerage.save(brokerageDict)

	#Only for output citation block
	def getAllCompanyCEODict(self):
		companyCEODict = {}
		for company in self.getAllCompany():
			CEOIdList = set(company['CEO'].itervalues())
			companyCEODict[company['code']] = ', '.join([self.getEngagerById(CEOId)['name'] for CEOId in CEOIdList])
		return companyCEODict

	def setArticleProcessed(self, articleId):
		self._db.article.update({'_id' : articleId}, {'$set' : {'processed' : True}})

	def setAllArticleUnprocessed(self):
		self._db.article.update({}, {'$set' : {'processed' : False}}, upsert=True, multi=True)

	def getUnprocessedArticleInBatch(self, limit=0):
		return self._db.article.find({'processed' : False}, timeout=False).limit(limit)

