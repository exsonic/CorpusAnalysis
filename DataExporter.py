"""
Created on 2013-5-10
@author: Bobi Pu, bobi.pu@usc.edu
"""
import csv, os, re
from DBController import DBController
from TextUtils import *
from nltk.tokenize import sent_tokenize
from threading import Thread
from Queue import Queue


class DataProcessorThread(Thread):
	def __init__(self, taskQueue, resultQueue, *args):
		super(DataProcessorThread, self).__init__()
		self._taskQueue = taskQueue
		self._resultQueue = resultQueue
		self._args = args
		self._executeFunction = None

		self._db = DBController()
		self._citeWordList = getWordList(CITE_WORD)
		if not os.path.exists('export/'):
			os.makedirs('export/')

	def exportSentenceAnalysis(self):
		#sentence collection is all the sentence
		#deprecated, need to refactor and apply queue
		with open('export/sentence.csv', 'wb') as f:
			writer = csv.writer(f)
			sentences = self._db.getAllSentence()
			articleDict = {}
			attributeList = ['id', 'cotic', 'coname', 'filePath', 'accessionNo', 'content', 'coname','ceoname', 'cite',
			                 'co_c', 'ceo_c', 'analyst_c', 'pfm', 'pfm_words', 'pos', 'pos_words', 'neg', 'neg_words',
			                 'internal', 'int_words', 'external', 'ext_words',
			                 'quote_sen', 'analyst']
			writer.writerow(attributeList)
			for i, sentence in enumerate(sentences):
				try:
					print(i)
					if sentence['articleId'] not in articleDict:
						articleDict[sentence['articleId']] = self._db.getArticleById(sentence['articleId'])
					article = articleDict[sentence['articleId']]
					articlePathPartList = article['filePath'].split('/')
					articleCompanyCode = articlePathPartList[-3] if articlePathPartList[-2] == 'a' else articlePathPartList[-2]
					articleCompany = self._db.getCompanyByCode(articleCompanyCode)
					articleCompanyName = articleCompanyCode if articleCompany is None else articleCompany['name']
					sentenceCompanyList = [self._db.getCompanyById(companyId) for companyId in sentence['company']]
					sentenceCompanyNameString = ','.join([company['shortName'] for company in sentenceCompanyList])
					sentenceEngagerList = [self._db.getEngagerById(engagerId) for engagerId in sentence['engager']]
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
					lineList = [sentence['_id'], articleCompanyCode, articleCompanyName, article['filePath'], article['_id'], sentence['content'].encode('utf-8'),
					            sentenceCompanyNameString, CEONameString, citeWordString, citeCompany, citeCEO, citeAnalyst,
					            len(sentence['pfm']), pfmWordString, len(sentence['pos']), posWordString, len(sentence['neg']), negWordString,
					            len(sentence['in']), inWordString, len(sentence['ex']), exWordString,
					            quoteString, analystSurroundString]

					writer.writerow(lineList)
				except Exception as e:
					print(e)

	def exportArticleAnalysis(self):
		#deprecated
		with open('export/article.csv', 'wb') as f:
			writer = csv.writer(f)
			articleList = list(self._db.getAllArticle())
			attributeList = ['cotic', 'coname', 'filePath', 'accessNo', 'date', 'source', 'byline',
			                 'coname1', 'coname2', 'coname3', 'coname4', 'coname5',
			                 'subjectCode1', 'subjectCode2', 'subjectCode3', 'subjectCode4', 'subjectCode5']
			writer.writerow(attributeList)
			for i, article in enumerate(articleList):
				try:
					print(i)
					articlePathPartList = article['filePath'].split('/')
					articleCompanyCode = articlePathPartList[-3] if articlePathPartList[-2] == 'a' else articlePathPartList[-2]
					articleCompany = self._db.getCompanyByCode(articleCompanyCode)
					articleCompanyName = articleCompanyCode if articleCompany is None else articleCompany['name']
					companyCodeList = [''] * ARTICLE_EXPORT_CODE_SIZE
					subjectCodeList = [''] * ARTICLE_EXPORT_CODE_SIZE
					if 'company' in article:
						for i, companyCode in enumerate(article['company']):
							if i >= ARTICLE_EXPORT_CODE_SIZE:
								break
							companyCodeList[i] = companyCode
					else:
						article['company'] = [articleCompanyCode]
						companyCodeList = article['company']

					if 'newsSubject' in article:
						for i, subjectCode in enumerate(article['newsSubject']):
							if i >= ARTICLE_EXPORT_CODE_SIZE:
								break
							subjectCodeList[i] = subjectCode
					else:
						article['newsSubject'] = []
						subjectCodeList = article['newsSubject']

					self._db.saveArticle(article)

					lineList = [articleCompanyCode, articleCompanyName, article['filePath'], article['_id'], article['date'], article['sourceName'], article['byline']] + companyCodeList + subjectCodeList
					writer.writerow(lineList)
				except Exception as e:
					print(e)

	def processKeywordSearch(self):
		searchString = self._args[0]
		while True:
			article = self._taskQueue.get()
			if article == END_OF_QUEUE:
				break
			else:
				articlePathPartList = article['filePath'].split('/')
				articleCompanyCode = articlePathPartList[-3] if articlePathPartList[-2] == 'a' else articlePathPartList[-2]
				articleCompany = self._db.getCompanyByCode(articleCompanyCode)
				articleCompanyName = articleCompanyCode if articleCompany is None else articleCompany['name']
				articleSentenceList = []

				#here, use '|' to combine regex is OK, because sentence is short, will not reduce the performance that much.
				#But in DB search, use iterative way.
				pattern = getPatternByKeywordSearchString(searchString)

				#on sentence level first, if can't find, go to paragraph level.
				for paragraph in [article['headline'], article['byline'], article['leadParagraph'], article['tailParagraph']]:
					sentenceList = sent_tokenize(paragraph)
					for sentence in sentenceList:
						if re.search(pattern, sentence) is not None:
							articleSentenceList.append(sentence.encode('utf-8').strip())
				if not articleSentenceList:
					#search on paragraph level
					for paragraph in [article['headline'], article['byline'], article['leadParagraph'], article['tailParagraph']]:
						if re.search(pattern, paragraph) is not None:
							articleSentenceList.append(paragraph.encode('utf-8').strip())
				lineList = [articleCompanyCode, articleCompanyName, article['filePath'], article['_id'], article['date'], article['sourceName'].strip(), article['byline'].strip(), article['headline'].strip(), '\t'.join(articleSentenceList)]
				self._resultQueue.put(lineList)


	def processCitationBlock(self):
		#because list is too long, we need to separate name in to chunk
		brokerNameList = list(self._db.getAllBrokerageEffectiveNameList())
		brokerageNamePatternList = []
		for i in range(0, len(brokerNameList), 500):
			brokerageNamePatternList.append(re.compile(r'|'.join([r'\b' + name + r'\b' for name in brokerNameList[i : i + 500]]), re.IGNORECASE))

		quotePattern = re.compile(r'\"[^\"]+\"')
		citeWordPatternStringList = [(r'\b' + citeWord + r'\b') for citeWord in self._citeWordList]

		companyCEODict = self._db.getAllCompanyCEODict()
		engagerNamePattern = re.compile(r'|'.join(['CEO', 'analyst', 'executive']), re.IGNORECASE)
		citeWordPattern = re.compile(r'|'.join(citeWordPatternStringList), re.IGNORECASE)

		while True:
			#process in batch
			articleBatch = self._taskQueue.get()
			if articleBatch == END_OF_QUEUE:
				self._taskQueue.task_done()
				break
			else:
				lineListBatch = []
				toProcessSentenceBatch = []
				#add byline_cleaned in articleDict
				self.processBylineInBatch(articleBatch)
				for article in articleBatch:
					self._db.setArticleProcessed(article['_id'])
					articlePathPartList = article['filePath'].split('/')
					articleCompanyCode = articlePathPartList[-3] if articlePathPartList[-2] == 'a' else articlePathPartList[-2]
					articleCompany = self._db.getCompanyByCode(articleCompanyCode)
					articleCompanyName = articleCompanyCode if articleCompany is None else articleCompany['name']
					articleLineListPart = [articleCompanyCode, articleCompanyName, article['filePath'], article['_id'], article['date'], article['sourceName'].strip(), article['byline'].strip(), article['byline_cleaned'], article['headline'].strip()]

					for paragraph in [article['leadParagraph'], article['tailParagraph']]:
						quotedStringList = re.findall(quotePattern, paragraph)
						if quotedStringList and max([len(string.split()) for string in quotedStringList]) > 5:
							sentenceList = sent_tokenize(paragraph)
							for sentence in sentenceList:
								quotedStringList = re.findall(quotePattern, sentence)
								citeWordList = re.findall(citeWordPattern, sentence)
								if quotedStringList and max([len(string.split()) for string in quotedStringList]) > 5 and citeWordList:
									lineList = articleLineListPart + [sentence, '. '.join(quotedStringList), ', '.join(citeWordList)]
									lineListBatch.append(lineList)
									toProcessSentenceBatch.append(sentence)
				actorAndOrgListBatch = self.processCiteSentenceInBatch(toProcessSentenceBatch)
				for i, actorAndOrgList in enumerate(actorAndOrgListBatch):
					if actorAndOrgList is not None:
						engagerNameList = re.findall(engagerNamePattern, lineListBatch[i][9])
						FCEO = 0
						articleCompanyCode = lineListBatch[i][0]
						for name in actorAndOrgList[0].split(', '):
							for namePart in name.split():
								if articleCompanyCode in companyCEODict and companyCEODict[articleCompanyCode].find(namePart) != -1:
									FCEO = 1
						lineListBatch[i] += (actorAndOrgList + [' '.join(engagerNameList)])
						lineListBatch[i].append(FCEO)
						unQuotedPart = re.sub(r'"[^"]+"', '', lineListBatch[i][9])
						findBrokerage = False
						for pattern in brokerageNamePatternList:
							result = pattern.search(unQuotedPart)
							if result is not None and result.string[result.regs[0][0]].isupper():
								# lineListBatch[i].append(result.string[result.regs[0][0] : result.regs[0][1]])
								findBrokerage = True
								break
						lineListBatch[i].append(1 if findBrokerage else 0)
						self._resultQueue.put(lineListBatch[i])
				self._taskQueue.task_done()

	def getNERTaggedTupleListFromSentence(self, sentence):
		#use senna name entity tagger, it fast!!
		sentence = unicode(sentence).encode('utf-8', 'ignore')
		with open('temp/input.txt', 'w') as f:
			f.write(sentence)
		os.system('./senna/senna -path senna/ -ner <temp/input.txt> temp/output.txt')
		with open('temp/output.txt', 'r') as f:
			tagTupleList = [[word.strip().split('-')[-1] if i ==1  else word.strip() for i, word in enumerate(line.split())] for line in f.readlines() if line.split()]
		return tagTupleList

	def processBylineInBatch(self, articleBatch):
		#use '.' to replace '' of byline, because if the last sentence byline is '', it will not be add to concatenated string.
		tagTupleList = self.getNERTaggedTupleListFromSentence(' ****** '.join([article['byline'] if article['byline'] else 'null.' for article in articleBatch]))

		personList, lastTag, wordList  = [], '', []
		articleIndex = 0
		for i in range(len(tagTupleList)):
			if tagTupleList[i][1] != lastTag:
				if lastTag == 'PER':
					personList.append(' '.join(wordList))
				wordList = [tagTupleList[i][0]]
				lastTag = tagTupleList[i][1]
			else:
				wordList.append(tagTupleList[i][0])

			if tagTupleList[i][0].find('****') != -1 or i == len(tagTupleList) - 1:
				#end of one sentence
				articleBatch[articleIndex]['byline_cleaned'] = ', '.join(personList) if personList else ''
				personList, lastTag, wordList = [], '', []
				articleIndex = articleIndex + 1 if i != len(tagTupleList) - 1 else articleIndex
				if articleIndex >= len(articleBatch):
					return

		while articleIndex < len(articleBatch) :
			articleBatch[articleIndex]['byline_cleaned'] = ''
			articleIndex += 1


	def processCiteSentenceInBatch(self, sentenceBatch):
		tagTupleList = self.getNERTaggedTupleListFromSentence(' ****** '.join(sentenceBatch))

		personAndOrgListBatch = []
		personList, orgnizationList, inQuoteFlag, lastTag, wordList  = [], [], False, '', []
		for i in range(len(tagTupleList)):
			if tagTupleList[i][0] == '\"':
				inQuoteFlag = 1 - inQuoteFlag
				if not inQuoteFlag:
					del wordList[:]
			else:
				if not inQuoteFlag:
					if tagTupleList[i][1] != lastTag:
						if lastTag == 'PER':
							personList.append(' '.join(wordList))
						elif lastTag == 'ORG':
							orgnizationList.append(' '.join(wordList))
						wordList = [tagTupleList[i][0]]
						lastTag = tagTupleList[i][1]
					else:
						wordList.append(tagTupleList[i][0])

			if tagTupleList[i][0].find('****') != -1 or i == len(tagTupleList) - 1:
				#end of one sentence
				if not personList and not orgnizationList:
					personAndOrgListBatch.append(None)
				else:
					personAndOrgListBatch.append([', '.join(personList), ', '.join(orgnizationList)])
				personList, orgnizationList, inQuoteFlag, lastTag, wordList  = [], [], False, '', []

		return personAndOrgListBatch

	def run(self):
		self._executeFunction()


class CSVWriterThread(Thread):
	def __init__(self, resultQueue, outputfilePath, attributeLineList, mode='w'):
		super(CSVWriterThread, self).__init__()
		self._resultQueue = resultQueue
		self._outputfilePath = outputfilePath
		self._attributeLineList = attributeLineList
		self._writeMode = mode

	def run(self):
		i = 0
		with open(self._outputfilePath, self._writeMode) as f:
			writer = csv.writer(f)
			writer.writerow(self._attributeLineList)
			while True:
				lineList = self._resultQueue.get()
				print(i)
				i += 1
				if lineList == END_OF_QUEUE:
					self._resultQueue.task_done()
					break
				else:
					try:
						writer.writerow(lineList)
					except Exception as e:
						print(e)
					finally:
						self._resultQueue.task_done()

class DataExporterMaster():
	def __init__(self):
		self._resultQueue = Queue()
		self._taskQueue = Queue()
		self._db = DBController()
		self._threadNumber = 4
		self._threadList = []


	def exportAllCitationBlock(self):
		#single thread is enough
		self._threadNumber = 1
		attributeList = ['cotic', 'coname', 'filePath', 'accessNo', 'date', 'source', 'byline', 'byline_cleaned', 'headline', 'sentence', 'cite_content', 'cite_word', 'actor', 'organization', 'engager', 'FCEO', 'broker']
		writer = CSVWriterThread(self._resultQueue, 'export/allCitationSentence.csv', attributeList, mode='w')
		writer.start()

		#must set to 100, otherwise there's bug
		batchSize = 100
		for i in range(self._threadNumber):
			t = DataProcessorThread(self._taskQueue, self._resultQueue)
			t._executeFunction = t.processCitationBlock
			t.start()
			self._threadList.append(t)

		while True:
			isDone = False
			for i in range(self._threadNumber):
				articleBatch = list(self._db.getAllUnprocessedArticle(batchSize))
				if articleBatch is None or not articleBatch:
					isDone = True
					break
				self._taskQueue.put(articleBatch)
			self._taskQueue.join()
			# break
			print('################')
			if isDone:
				break

		for i in range(self._threadNumber):
			self._taskQueue.put(END_OF_QUEUE)

		self._taskQueue.join()
		for t in self._threadList:
			t.join()
		self._resultQueue.put(END_OF_QUEUE)
		self._resultQueue.join()
		writer.join()

	def exportKeywordSearch(self, searchString):
		self._threadNumber = 4
		attributeList = ['cotic', 'coname', 'filePath', 'accessNo', 'date', 'source', 'byline', 'headline', 'sentence']
		writer = CSVWriterThread(self._resultQueue, 'export/keywordSearch.csv', attributeList)
		writer.start()

		for i in range(self._threadNumber):
			t = DataProcessorThread(self._taskQueue, self._resultQueue, searchString)
			t._executeFunction = t.processKeywordSearch
			t.start()
			self._threadList.append(t)

		articleListCursor = self._db.getAllArticleBySearchString(searchString)
		#it's cursor here!!
		for article in articleListCursor:
			self._taskQueue.put(article)

		for i in range(self._threadNumber):
			self._taskQueue.put(END_OF_QUEUE)

		for t in self._threadList:
			t.join()
		self._resultQueue.put(END_OF_QUEUE)
		writer.join()


if __name__ == '__main__':
	master = DataExporterMaster()
	master.exportAllCitationBlock()