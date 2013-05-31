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
				sentences = self.db.getAllSentenceIterForOneType(keyValue)
				sentenceList = getProcessedSentenceList(sentences)
				transformer = CountVectorizer()
				wordCountArray = transformer.fit_transform(sentenceList).toarray()
				wordCountList = sum(wordCountArray, axis=0).tolist()
				wordTable = transformer.get_feature_names()
				frequentWordList = self.getTopWordList(wordCountList, wordTable, limit)
				lineList.extend(frequentWordList)
				writer.writerow(lineList)

	def exportSentenceAnalysis(self):
		#from pfm*neg*atrb
		sentences = self.db.getAllPfmNegSentenceIterWithAtrb()
		with open('export/sentence.csv', 'wb') as f:
			writer = csv.writer(f)
			atrbWordList = getAtrbWordList()
			attributeList = ['id', 'accessionNo', 'content', 'year', 'wordCount', 'in', 'ex', 'pfm',
			                 'neg', 'CEO', 'analyst', 'company', 'cite'] + atrbWordList
			writer.writerow(attributeList)
			for i, sentence in enumerate(sentences):
				print(i)
				atrbWordDict = getAtrbWordDict()
				CEOCount, analystCount, year = 0, 0, self.db.getSentenceDate(sentence['articleId']).year
				companyCount = len(sentence['company']) if 'company' in sentence else 0
				if 'engager' in sentence:
					for engagerId in sentence['engager']:
						if self.db.getEngagerById(engagerId)['type'] == ENGAGER_CEO:
							CEOCount += 1
						else:
							analystCount += 1
				citeCount = len(sentence[getKeyFromWordType(CITE_WORD)]) if getKeyFromWordType(CITE_WORD) in sentence else 0

				for word in sentence[getKeyFromWordType(ATRB_EX)]:
					atrbWordDict[word] += 1
				for word in sentence[getKeyFromWordType(ATRB_IN)]:
					atrbWordDict[word] += 1
				lineList = [sentence['id'], sentence['articleId'], sentence['content'].encode(ENCODE_UTF8), year, getStringWordCount(sentence['content']),
				            len(sentence['in']), len(sentence['ex']), len(sentence['pfm']), len(sentence['neg']), CEOCount, analystCount, companyCount, citeCount]
				for word in atrbWordList:
					lineList.append(atrbWordDict[word])
				writer.writerow(lineList)

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
			sentences = self.db.getAllSentenceIter()
			for sentence in sentences:
				if sentence[key]:
					lineList = [sentence['id'], sentence['articleId'], sentence['paragraph'], sentence['content'],
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
		sentences = self.db.getAllSentenceIterWithArticleId(self.articleId)
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
	de.exportSentenceAnalysis()