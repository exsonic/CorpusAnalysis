"""
Created on 2013-5-9
@author: Bobi Pu, bobi.pu@usc.edu
"""

from nltk.tokenize import sent_tokenize
from textUtils import getWordList, getLemmatizer, getProcessedWordList, isPfmString
from DBController import DBController
from setting import *

class SignifierParser(object):
	def __init__(self):
		self.db = DBController()
		self.lemmatizer = getLemmatizer()
		self.pfmWord = getWordList(WORD_PFM)
		self.posWord = getWordList(WORD_POS)
		self.negWord = getWordList(WORD_NEG)
		self.exWord = getWordList(ATRB_EX)
		self.inWord = getWordList(ATRB_IN)

	def parseAllPfmSentence(self):
		articles = self.db.getAllArticleIter()
		parsedArticleIdDict = self.db.getParsedArticleIdDict()
		for article in articles:
			if article['id'] in parsedArticleIdDict:
				continue
			else:
				parsedArticleIdDict[article['id']] = True
				paragraphSet = ('byline', 'leadParagraph', 'tailParagraph')
				for key in paragraphSet:
					paragraph = article[key]
					sentenceList = sent_tokenize(paragraph)
					for sentence in sentenceList:
						sentenceDict = self.parsePfmSentence(sentence)
						if sentenceDict is not None:
							sentenceDict['articleId'] = article['id']
							sentenceDict['paragraph'] = key
							self.db.insertSentence(sentenceDict)

	def parsePfmSentence(self, string):
		if not isPfmString(string):
			return None
		words = getProcessedWordList(string)
		pfmWordList = filter(lambda word : word in self.pfmWord, words)
		if not pfmWordList:
			return None
		else:
			posWordList = filter(lambda word : word in self.posWord, words)
			negWordList = filter(lambda word : word in self.negWord, words)
			if not posWordList and not negWordList:
				return None
			else:
				sentenceDict = {'pfm' : pfmWordList, 'pos' : posWordList, 'neg' : negWordList, 'content' : string.encode(ENCODE_UTF8)}
				return sentenceDict

	def parseAllSentenceAtrb(self):
		sentences = self.db.getAllSentenceIterWithoutAtrb()
		for i, sentence in enumerate(sentences):
			print(i)
			self.parseSentenceAtrb(sentence)

	def parseSentenceAtrb(self, sentence):
		words = getProcessedWordList(sentence['content'])
		exWordList = filter(lambda word : word in self.exWord, words)
		inWordList = filter(lambda word : word in self.inWord, words)
		self.db.updateSentenceAtrb(sentence['id'], exWordList, inWordList)

	def parseEngager(self):
		for i, engager in enumerate(self.db.getAllEngager()):
			print(i)
			searchName = engager['lastName']
			if engager['lastName'] == 'Jones':
				searchName = engager['fullName']
			for sentence in self.db.getAllSentenceWithString(searchName):
				#id 91 92 are analyst
				self.db.updateSentenceEngager(sentence['id'], engager['id'])

	def parseCompany(self):
		for i, company in enumerate(self.db.getAllCompany()):
			print(i)
			upperCaseName, normalName = company['shortName'], company['shortName'].title()
			for sententce in self.db.getAllSentenceWithString(upperCaseName):
				self.db.updateSentenceCompany(sentence['id'], company['id'])
			for sentence in self.db.getAllSentenceWithString(normalName):
				self.db.updateSentenceCompany(sentence['id'], company['id'])

	def parseCiteWord(self):
		for i, word in enumerate(getWordList(CITE_WORD)):
			print(i)
			for sentence in self.db.getAllSentenceWithString(word):
				self.db.updateSentenceCiteWord(sentence['id'], word)


if __name__ == '__main__':
	sp = SignifierParser()
	# sp.parseAllSentenceAtrb()
	# sp.parseEngager()
	# sp.parseCompany()
	sp.parseCiteWord()
	#export
