"""
Created on 2013-5-10
@author: Bobi Pu, bobi.pu@usc.edu
"""

from DBController import DBController
import csv
import os
from textUtils import *
from setting import *
from threading import Thread, activeCount
from Queue import Queue
from sklearn.feature_extraction.text import CountVectorizer
from numpy import sum
from fileUtils import getAtrbTypeKeyFromFolderName


class DataExporter(object):
	def __init__(self):
		self.db = DBController()
		if not os.path.exists('export/'):
			os.makedirs('export/')

	def exportFrequentWordFromSentence(self, limit=100, isAtrbSentence=False):
		fileName = 'freqAtrbWord.csv' if isAtrbSentence else 'freqPfmWord.csv'
		with open('export/' + fileName, 'wb') as f:
			writer = csv.writer(f)
			attributeList = ['type']
			attributeList.extend(['word'] * limit)
			writer.writerow(attributeList)
			keyValueList = [ATRB_NO, ATRB_IN, ATRB_EX] if isAtrbSentence else [WORD_PFM, WORD_POS, WORD_NEG]
			for keyValue in keyValueList:
				key = getKeyFromWordType(keyValue)
				lineList = [key]
				sentences = self.db.getAllSentenceForOneType(keyValue)
				sentenceList = getProcessedSentenceList(sentences)
				transformer = CountVectorizer()
				wordCountArray = transformer.fit_transform(sentenceList).toarray()
				wordCountList = sum(wordCountArray, axis=0).tolist()
				wordTable = transformer.get_feature_names()
				frequentWordList = self.getTopWordList(wordCountList, wordTable, limit)
				lineList.extend(frequentWordList)
				writer.writerow(lineList)

	#sentence collection is all the sentence
	def exportSentenceAnalysis(self):
		with open('export/sentence.csv', 'wb') as f:
			writer = csv.writer(f)
			sentences = self.db.getAllSentence()
			articleDict = {}
			attributeList = ['id', 'cotic', 'coname', 'filePath', 'accessionNo', 'content', 'coname','ceoname', 'cite',
			                 'co_c', 'ceo_c', 'analyst_c', 'pfm', 'pfm_words', 'pos', 'pos_words', 'neg', 'neg_words',
			                 'int', 'int_words', 'ext', 'ext_words',
			                 'quote_sen', 'analyst']
			writer.writerow(attributeList)
			for i, sentence in enumerate(sentences):
				try:
					print(i)
					if sentence['articleId'] not in articleDict:
						articleDict[sentence['articleId']] = self.db.getArticleById(sentence['articleId'])
					article = articleDict[sentence['articleId']]
					articleCompanyCode = article['filePath'].split('/')[2]
					articleCompany = self.db.getCompanyByCode(articleCompanyCode)
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
					lineList = [sentence['_id'], articleCompanyCode, articleCompany['name'], article['filePath'], article['id'], sentence['content'],
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
			articles = self.db.getAllArticle()
			attributeList = ['cotic', 'coname', 'filePath', 'accessNo', 'date', 'source', 'author',
			                 'coname1', 'coname2', 'coname3', 'coname4', 'coname5',
			                 'subjectCode1', 'subjectCode2', 'subjectCode3', 'subjectCode4', 'subjectCode5']
			writer.writerow(attributeList)
			for i, article in enumerate(articles):
				try:
					print(i)
					articleCompanyCode = article['filePath'].split('/')[2]
					articleCompany = self.db.getCompanyByCode(articleCompanyCode)
					companyCodeList = [''] * ARTICLE_EXPORT_CODE_SIZE
					subjectCodeList = [''] * ARTICLE_EXPORT_CODE_SIZE
					for i, companyCode in enumerate(article['company']):
						if i >= ARTICLE_EXPORT_CODE_SIZE:
							break
						companyCodeList[i] = companyCode
					for i, subjectCode in enumerate(subjectCodeList):
						if i >= ARTICLE_EXPORT_CODE_SIZE:
							break
						subjectCodeList[i] = subjectCode

					lineList = [articleCompanyCode, articleCompany['name'], article['filePath'], article['_id'], article['date'], article['byline']] + companyCodeList + subjectCodeList
					writer.writerow(lineList)
				except Exception as e:
					print(e)


	def getTopWordList(self, wordCountList, wordTable, limit):
		topWordList = []
		for _ in range(limit):
			index = wordCountList.index(max(wordCountList))
			topWordList.append(wordTable[index])
			wordCountList[index] = 0
		return topWordList

	def exportPosNegSentence(self, wordType):
		fileName = 'posSentence.csv' if wordType == WORD_POS else 'negSentence.csv'
		key = getKeyFromWordType(wordType)
		attributeList = ['id', 'accessionNo', 'paragraph', 'content', 'pfmWord']
		if wordType == WORD_POS:
			attributeList.append('posWord')
		else:
			attributeList.append('negWord')
		wordList = getWordList(wordType)
		attributeList.extend(wordList)
		with open('export/' + fileName, 'wb') as f:
			writer = csv.writer(f)
			writer.writerow(attributeList)
			sentences = self.db.getAllSentence()
			for sentence in sentences:
				if sentence[key]:
					lineList = [sentence['_id'], sentence['articleId'], sentence['paragraph'], sentence['content'],
					            ' '.join(sentence['pfm']), ' '.join(set(sentence[key]))]
					wordDict = getWordDict(wordType)
					wordDict = countWord(sentence[key], wordDict)
					lineList.extend(getWordCountList(wordList, wordDict))
					lineList = [value.encode('utf-8') if isinstance(value, unicode) else value for value in lineList]
					writer.writerow(lineList)

	def exportArticleWithWordFrequency(self):
		attributeList = ['accessionNo', 'pfmWordCount', 'posWordCount', 'negWordCount']
		pfmWordList, posWordList, negWordList = getWordList(WORD_PFM), getWordDict(WORD_POS), getWordList(WORD_NEG)
		attributeList.extend(pfmWordList)
		attributeList.extend(posWordList)
		attributeList.extend(negWordList)
		with open('export/article.csv', 'wb') as f:
			writer = csv.writer(f)
			writer.writerow(attributeList)
			articleIds = self.db.getALLArticleIdWithPfm()
			queue = Queue()
			writeThread = WriteCSVLineThread(writer, queue)
			writeThread.daemon = True
			writeThread.start()
			for articleId in articleIds:
				while activeCount() > MAX_CONNECTION_SIZE:
					pass
				thread = ExportArticleWordThread(queue, articleId, pfmWordList, posWordList, negWordList)
				thread.start()
			queue.join()

#used in exportArticleWordFrequency to speed up
class WriteCSVLineThread(Thread):
	def __init__(self, writer, queue):
		super(WriteCSVLineThread, self).__init__()
		self.writer = writer
		self.queue = queue

	def run(self):
		while True:
			lineList = self.queue.get()
			self.writer.writerow(lineList)
			self.queue.task_done()

#used in exportArticleWordFrequency to speed up
class ExportArticleWordThread(Thread):
	def __init__(self, queue, articleId, pfmWordList, posWordList, negWordList):
		super(ExportArticleWordThread, self).__init__()
		self.queue, self.db, self.articleId = queue, DBController(), articleId
		self.pfmWordList, self.posWordList, self.negWordList = pfmWordList, posWordList, negWordList

	def run(self):
		lineList = [self.articleId]
		sentences = self.db.getAllSentenceWithArticleId(self.articleId)
		pfmWordDict, posWordDict, negWordDict = getWordDictWithWordList(self.pfmWordList), getWordDictWithWordList(
			self.posWordList), getWordDictWithWordList(self.negWordList)
		pfmSentenceWordList, posSentenceWordList, negSentenceWordList = [], [], []
		pfmKey, posKey, negKey = getKeyFromWordType(WORD_PFM), getKeyFromWordType(WORD_POS), getKeyFromWordType(
			WORD_NEG)
		for sentence in sentences:
			pfmSentenceWordList.extend(sentence[pfmKey])
			posSentenceWordList.extend(sentence[posKey])
			negSentenceWordList.extend(sentence[negKey])
		pfmWordDict = countWord(pfmSentenceWordList, pfmWordDict)
		posWordDict = countWord(posSentenceWordList, posWordDict)
		negWordDict = countWord(negSentenceWordList, negWordDict)
		lineList.append(len(pfmSentenceWordList))
		lineList.append(len(posSentenceWordList))
		lineList.append(len(negSentenceWordList))
		lineList.extend(getWordCountList(self.pfmWordList, pfmWordDict))
		lineList.extend(getWordCountList(self.posWordList, posWordDict))
		lineList.extend(getWordCountList(self.negWordList, negWordDict))
		self.queue.put(lineList)

if __name__ == '__main__':
	de = DataExporter()
	# de.exportSentenceAnalysis()
	de.exportArticleAnalysis()