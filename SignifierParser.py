"""
Created on 2013-5-9
@author: Bobi Pu, bobi.pu@usc.edu
"""
from Queue import Queue
from threading import Thread, activeCount
from nltk import WordNetLemmatizer, word_tokenize

from nltk.tokenize import sent_tokenize
from TextUtils import getWordList, getProcessedWordList, isValidSentence, wrapWord
from DBController import DBController
from Setting import *

class SignifierParser(object):

	def __init__(self):
		self.db = DBController()
		self.pfmWord = getWordList(WORD_PFM)
		self.posWord = getWordList(WORD_POS)
		self.negWord = getWordList(WORD_NEG)
		self.exWord = getWordList(ATRB_EX)
		self.inWord = getWordList(ATRB_IN)
		self.citeWord = getWordList(CITE_WORD)
		self.engagers = list(self.db.getAllEngager())
		self.companies = list(self.db.getAllCompany())

	def extractAllSentenceToDB(self, isReload=False):
		if isReload:
			self.db.dropSentence()
		for company in self.companies:
			articles = self.db.getAllArticleByCompanyCode(company['code'])
			engagers = self.db.getAllEngagerByCompanyId(company['_id'])
			for i, article in enumerate(articles):
				print(i)
				paragraphSet = ('byline', 'leadParagraph', 'tailParagraph')
				for key in paragraphSet:
					paragraph = article[key]
					sentenceList = sent_tokenize(paragraph)
					for string in sentenceList:
						if not isValidSentence(string):
							continue
						sentenceDict = {'content' : string.encode(ENCODE_UTF8), 'articleId' : article['_id'], 'paragraph' : key}
						sentenceDict = self.parseRawSentence(sentenceDict, engagers, self.companies)
						if sentenceDict is not None:
							self.db.insertSentence(sentenceDict)

	def parseRawSentence(self, sentence, engagers, companies):
		engagerIdList, companyIdList = [], []
		searchString = sentence['content']
		for engager in engagers:
			try:
				if engager['lastName'] == 'Jones' or engager['lastName'] == 'Johnson' or engager['lastName'] == 'West' or engager['lastName'] == 'Post' or engager['lastName'] == 'Ford':
					searchName = wrapWord(engager['name'])
				else:
					searchName = wrapWord(engager['lastName'])
				if searchString.find(searchName) != -1:
					engagerIdList.append(engager['_id'])
			except:
				pass

		for company in companies:
			try:
				searchName = wrapWord(company['shortName']).title()
				if searchString.find(searchName) != -1:
					companyIdList.append(company['_id'])

				searchName = wrapWord(company['shortName']).upper()
				if searchString.find(searchName) != -1:
					companyIdList.append(company['_id'])
			except:
				pass

		if not engagerIdList and not companyIdList:
			return None
		else:
			sentence['engager'] = list(set(engagerIdList))
			sentence['company'] = list(set(companyIdList))
			return sentence

	# def parseAllSentenceEngagerCiteDistance(self):
	# 	sentences = list(self.db.getAllSentence())
	# 	for i, sentence in enumerate(sentences):
	# 		print(i)
	# 		isCiteCEO, isCiteAnalyst, isCiteCompany = self.isCiteInDistance(sentence)
	# 		self.db.updateCiteDistance(sentence['_id'], isCiteCEO, isCiteAnalyst, isCiteCompany)

	def parseAllSentenceCitation(self):
		sentences = list(self.db.getAllSentence())
		for i, sentence in enumerate(sentences):
			print(i)
			words = getProcessedWordList(sentence['content'], VERB)
			sentence['cite'] = filter(lambda  word : word in self.citeWord, words)
			sentence['citeCEO'], sentence['citeAnalyst'], sentence['citeCompany'] = self.isCiteInDistance(sentence)
			self.db.saveSentence(sentence)

	def parseAllSentenceEngagerCompany(self):
		sentences = self.db.getAllSentence()
		for sentence in sentences:
			engagerIdList, companyIdList = [], []
			searchString = sentence['content']
			for engager in self.engagers:
				if engager['lastName'] == 'Jones' or engager['lastName'] == 'Johnson' or engager['lastName'] == 'West' or engager['lastName'] == 'Post' or engager['lastName'] == 'Ford' or engager['lastName'] == 'Collins':
					searchName = wrapWord(engager['name']).lower()
				else:
					searchName = wrapWord(engager['lastName']).lower()
				if searchString.find(searchName) != -1:
					engagerIdList.append(engager['_id'])

			for company in self.companies:
				searchName = wrapWord(company['shortName']).lower()
				if searchString.find(searchName) != -1:
					companyIdList.append(company['_id'])

			sentence['engager'], sentence['company'] = list(set(engagerIdList)), list(set(companyIdList))
			self.db.saveSentence(sentence)

	def parseAllSentencePfm(self):
		#list them all, becaue if loop with cursor and update cursor pointed sentence at meantime, the cursor will be screwed.
		sentences = list(self.db.getAllSentence())
		for i, sentence in enumerate(sentences):
			print(i)
			pfmSentenceWordList = getProcessedWordList(sentence['content'], NOUN)
			pfmWordList = filter(lambda word : word in self.pfmWord, pfmSentenceWordList)
			posNegSentenceWordList = getProcessedWordList(sentence['content'], VERB)
			posWordList = filter(lambda word : word in self.posWord, posNegSentenceWordList)
			negWordList = filter(lambda word : word in self.negWord, posNegSentenceWordList)

			posWordList, negWordList = self.filterPosNegWordListByDistance(pfmSentenceWordList, posNegSentenceWordList, pfmWordList, posWordList, negWordList)

			self.db.updateSentencePfm(sentence['_id'], pfmWordList, posWordList, negWordList)

	def parseAllSentenceAtrb(self):
		sentences = list(self.db.getAllSentence())
		for i, sentence in enumerate(sentences):
			print(i)
			words = getProcessedWordList(sentence['content'], NOUN)
			exWordList = filter(lambda word : word in self.exWord, words)
			inWordList = filter(lambda word : word in self.inWord, words)
			if ('ceo' in inWordList or 'executive' in inWordList) and sentence['cite']:
				inWordList = []
			self.db.updateSentenceAtrb(sentence['_id'], exWordList, inWordList)

	def isCiteInDistance(self, sentence):
		#if (CEO or Company) and citation word happens within 5 word distance, capture
		isCiteCEO, isCiteAnalyst, isCiteCompany = False, False, False
		if sentence['cite']:
			wordList = getProcessedWordList(sentence['content'], VERB)
			for citeWord in sentence['cite']:
				citeIndex = wordList.index(citeWord)
				for engagerId in sentence['engager']:
					try:
						engager = self.db.getEngagerById(engagerId)
						matchName = engager['lastName'].lower()
						engagerIndex = wordList.index(matchName)
						if abs(citeIndex - engagerIndex) <= CITE_DISTANCE:
							if engager['type'] == ENGAGER_CEO:
								isCiteCEO = True
							else:
								isCiteAnalyst = True
					except:
						pass

				for companyId in sentence['company']:
					try:
						company = self.db.getCompanyById(companyId)
						matchName = company['shortName'].lower()
						companyIndex = wordList.index(matchName)
						if abs(citeIndex - companyIndex) <= CITE_DISTANCE:
							isCiteCompany = True
					except:
						pass
		return isCiteCEO, isCiteAnalyst, isCiteCompany

	def filterPosNegWordListByDistance(self, pfmSentenceWordList, posNegSentenceWordList, pfmWordList, posWordList, negWordList):
		filteredPosWordList, filteredNegWordList = [],[]
		for pfmWord in pfmWordList:
			pfmIndex = pfmSentenceWordList.index(pfmWord)
			for posWord in posWordList:
				posIndex = posNegSentenceWordList.index(posWord)
				if abs(pfmIndex - posIndex) <= PFM_DISTANCE:
					filteredPosWordList.append(posWord)
			for negWord in negWordList:
				negIndex = posNegSentenceWordList.index(negWord)
				if abs(pfmIndex - negIndex) <= PFM_DISTANCE:
					filteredNegWordList.append(negWord)
		return filteredPosWordList, filteredNegWordList



# if __name__ == '__main__':
	# sp.extractAllSentenceToDB(True)
	# sp = SignifierParser()
	# sp.parseAllSentenceCitation()
	# sp.parseAllSentencePfm()
	# sp.parseAllSentenceAtrb()

	#BACKUP AND REWRITE OUTPUT!!!