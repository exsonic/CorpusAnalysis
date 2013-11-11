"""
Created on 2013-5-9
@author: Bobi Pu, bobi.pu@usc.edu
"""
import re

from nltk.tokenize import sent_tokenize
from TextUtils import getWordList, getProcessedWordList, isValidSentence
from DBController import DBController
from Settings import *

class SignifierParser(object):

	def __init__(self):
		self.db = DBController()
		self.pfmWord = getWordList(WORD_PFM)
		self.posWord = getWordList(WORD_POS)
		self.negWord = getWordList(WORD_NEG)
		self.exWord = getWordList(ATRB_EX)
		self.inWord = getWordList(ATRB_IN)
		self.citeWord = getWordList(WORD_CITE)
		self.engagerList = list(self.db.getAllEngager())
		self.companyList = list(self.db.getAllCompany())

		self.engagerRegexPatternDict, self.companyRegexPatternDict = self.getRegexPatternDictForEngagerAndCompany()

	def getRegexPatternDictForEngagerAndCompany(self):
		engagerRegexPatternDict, companyRegexPatternDict = {}, {}
		for engager in self.engagerList:
			if engager['lastName'] == 'Jones' or engager['lastName'] == 'Johnson' or engager['lastName'] == 'West' or engager['lastName'] == 'Post' or engager['lastName'] == 'Ford':
				searchName = engager['name']
			else:
				searchName = engager['lastName']
			engagerRegexPatternDict[engager['_id']] = re.compile(r'\b' + searchName + r'\b')

		for company in self.companyList:
			companyRegexPatternDict[company['_id']] = re.compile(r'\b' + company['shortName'] + r'\b', re.IGNORECASE)

		return engagerRegexPatternDict, companyRegexPatternDict

	def extractAllSentenceToDB(self, isReload=False):
		if isReload:
			self.db.dropSentence()
		# for company in self.companies:
		for i, company in enumerate(self.companyList):
			articles = list(self.db.getAllArticleByCompanyCode(company['code']))
			engagers = list(self.db.getAllEngagerByCompanyId(company['_id']))
			for j, article in enumerate(articles):
				print(i, j)
				paragraphSet = ('leadParagraph', 'tailParagraph')
				for key in paragraphSet:
					paragraph = article[key]
					sentenceList = sent_tokenize(paragraph)
					for string in sentenceList:
						if not isValidSentence(string):
							continue
						sentenceDict = {'content' : string.encode('utf-8'), 'articleId' : article['_id'], 'paragraph' : key}
						sentenceDict = self.parseRawSentence(sentenceDict, engagers)
						if sentenceDict is not None:
							self.db.insertSentence(sentenceDict)

	def parseRawSentence(self, sentenceDict, engagerList):
		engagerIdList, companyIdList = [], []
		for engager in engagerList:
			if self.engagerRegexPatternDict[engager['_id']].search(sentenceDict['content']) is not None:
				engagerIdList.append(engager['_id'])

		for company in self.companyList:
			if self.companyRegexPatternDict[company['_id']].search(sentenceDict['content']) is not None:
				companyIdList.append(company['_id'])

		if not engagerIdList and not companyIdList:
			return None
		else:
			sentenceDict['engager'] = list(set(engagerIdList))
			sentenceDict['company'] = list(set(companyIdList))
			return sentenceDict

	def parseAllSentenceCitation(self):
		sentences = list(self.db.getAllSentence())
		for i, sentence in enumerate(sentences):
			print(i)
			words = getProcessedWordList(sentence['content'], VERB)
			sentence['cite'] = filter(lambda  word : word in self.citeWord, words)
			sentence['citeCEO'], sentence['citeAnalyst'], sentence['citeCompany'] = self.isCiteInDistance(sentence)
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



if __name__ == '__main__':
	sp = SignifierParser()
	# sp.extractAllSentenceToDB(True)
	# sp.parseAllSentenceCitation()
	# sp.parseAllSentencePfm()
	sp.parseAllSentenceAtrb()

	#BACKUP AND REWRITE OUTPUT!!!