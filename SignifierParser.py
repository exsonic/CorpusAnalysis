"""
Created on 2013-5-9
@author: Bobi Pu, bobi.pu@usc.edu
"""

from nltk.tokenize import sent_tokenize
from textUtils import getWordList, getLemmatizer, getProcessedWordList, isValidSentence, wrapWord
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

	#get all sentence by engager and company
	def parseSentenceToDB(self):
		articles = self.db.getAllArticle()
		parsedArticleIdDict = self.db.getParsedArticleIdDict()
		for i, article in enumerate(articles):
			print(i)
			if article['id'] in parsedArticleIdDict:
				continue
			else:
				parsedArticleIdDict[article['id']] = True
				paragraphSet = ('byline', 'leadParagraph', 'tailParagraph')
				for key in paragraphSet:
					paragraph = article[key]
					sentenceList = sent_tokenize(paragraph)
					for sentence in sentenceList:
						sentenceDict = self.parseEngagerCompanySentence(sentence)
						if sentenceDict is not None:
							sentenceDict['articleId'] = article['id']
							sentenceDict['paragraph'] = key
							self.db.insertSentence(sentenceDict)

	def parseEngagerCompanySentence(self, string):
		if not isValidSentence(string):
			return None
		engagerIdList, companyIdList, engagers, companys = [], [], self.db.getAllEngager(), self.db.getAllCompany()
		for engager in engagers:
			if engager['lastName'] == 'Jones' or engager['lastName'] == 'Johnson' or engager['lastName'] == 'West' or engager['lastName'] == 'Post' or engager['lastName'] == 'Ford':
				searchName = wrapWord(engager['fullName'])
			else:
				searchName = wrapWord(engager['lastName'])
			if string.find(searchName) != -1:
				engagerIdList.append(engager['id'])
		for company in companys:
			upperCaseName, normalName = wrapWord(company['shortName']), wrapWord(company['shortName'].title())
			if string.find(upperCaseName) != -1:
				companyIdList.append(company['id'])
			elif string.find(normalName) != -1:
				companyIdList.append(company['id'])
		if not engagerIdList and not companyIdList:
			return None
		else:
			#remove duplication
			engagerIdList, companyIdList = list(set(engagerIdList)), list(set(companyIdList))
			sentenceDict = {'content' : string.encode(ENCODE_UTF8), 'engager' : engagerIdList, 'company' : companyIdList}
			return sentenceDict

	def parseAllSentencePfm(self):
		#list them all, becaue if loop with cursor and update cursor pointed sentence at meantime, the cursor will be screwed.
		sentences = list(self.db.getAllSentence())
		for i, sentence in enumerate(sentences):
			print(i)
			words = getProcessedWordList(sentence['content'])
			pfmWordList = filter(lambda word : word in self.pfmWord, words)
			posWordList = filter(lambda word : word in self.posWord, words)
			negWordList = filter(lambda word : word in self.negWord, words)
			self.db.updateSentencePfm(sentence['id'], pfmWordList, posWordList, negWordList)

	def parseAllSentenceAtrb(self):
		sentences = self.db.getAllSentence()
		for i, sentence in enumerate(sentences):
			print(i)
			words = getProcessedWordList(sentence['content'])
			exWordList = filter(lambda word : word in self.exWord, words)
			inWordList = filter(lambda word : word in self.inWord, words)
			self.db.updateSentenceAtrb(sentence['id'], exWordList, inWordList)

	def parseAllSentenceCitation(self):
		for i, word in enumerate(getWordList(CITE_WORD)):
			print(i)
			for sentence in self.db.getAllSentenceWithWord(word):
				self.db.updateSentenceCiteWord(sentence['id'], word)

if __name__ == '__main__':
	sp = SignifierParser()
	# sp.parseSentenceToDB()
	sp.parseAllSentencePfm()
	# sp.parseAllSentenceAtrb()
	# sp.parseAllSentenceCitation()

	#BACKUP AND REWRITE OUTPUT!!!