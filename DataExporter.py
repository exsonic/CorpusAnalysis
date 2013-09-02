"""
Created on 2013-5-10
@author: Bobi Pu, bobi.pu@usc.edu
"""
import re

from DBController import DBController
import csv
import os
from TextUtils import *
from nltk.tokenize import sent_tokenize
from nltk.tag.stanford import NERTagger


class DataExporter(object):
	def __init__(self):
		self.db = DBController()
		self.NERTagger = NERTagger('stanford-ner/classifiers/english.all.3class.distsim.crf.ser.gz', 'stanford-ner/stanford-ner.jar')
		self.citeWordList = getWordList(CITE_WORD)
		if not os.path.exists('export/'):
			os.makedirs('export/')

	#sentence collection is all the sentence
	def exportSentenceAnalysis(self):
		with open('export/sentence.csv', 'wb') as f:
			writer = csv.writer(f)
			sentences = self.db.getAllSentence()
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
						articleDict[sentence['articleId']] = self.db.getArticleById(sentence['articleId'])
					article = articleDict[sentence['articleId']]
					articleCompanyCode = article['filePath'].split('/')[-2]
					articleCompany = self.db.getCompanyByCode(articleCompanyCode)
					articleCompanyName = articleCompanyCode if articleCompany is None else articleCompany['name']
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
					lineList = [sentence['_id'], articleCompanyCode, articleCompanyName, article['filePath'], article['_id'], sentence['content'].encode('utf-8'),
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
			articleList = list(self.db.getAllArticle())
			attributeList = ['cotic', 'coname', 'filePath', 'accessNo', 'date', 'source', 'byline',
			                 'coname1', 'coname2', 'coname3', 'coname4', 'coname5',
			                 'subjectCode1', 'subjectCode2', 'subjectCode3', 'subjectCode4', 'subjectCode5']
			writer.writerow(attributeList)
			for i, article in enumerate(articleList):
				try:
					print(i)
					articleCompanyCode = article['filePath'].split('/')[-2]
					articleCompany = self.db.getCompanyByCode(articleCompanyCode)
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

					self.db.saveArticle(article)

					lineList = [articleCompanyCode, articleCompanyName, article['filePath'], article['_id'], article['date'], article['sourceName'], article['byline']] + companyCodeList + subjectCodeList
					writer.writerow(lineList)
				except Exception as e:
					print(e)

	def exportKeywordSearch(self, searchString):
		articleList = list(self.db.getAllArticleBySearchString(searchString))
		print(str(len(articleList)) + ' articles retrieved.')
		if articleList:
			with open('export/keywordSearch.csv', 'wb') as f:
				writer = csv.writer(f)
				attributeList = ['cotic', 'coname', 'filePath', 'accessNo', 'date', 'source', 'byline', 'headline', 'sentence']
				writer.writerow(attributeList)
				for i, article in enumerate(articleList):
					try:
						print(i)
						articleCompanyCode = article['filePath'].split('/')[-2]
						articleCompany = self.db.getCompanyByCode(articleCompanyCode)
						articleCompanyName = articleCompanyCode if articleCompany is None else articleCompany['name']
						articleSentenceList = []
						if searchString.find(',') == -1:
							pattern = re.compile(r'\b' + searchString + r'\b', re.IGNORECASE)
							for paragraph in [article['headline'], article['byline'], article['leadParagraph'], article['tailParagraph']]:
								sentenceList = sent_tokenize(paragraph)
								for sentence in sentenceList:
									if re.search(pattern, sentence) is not None:
										articleSentenceList.append(sentence)
						else:
							regexString = ''
							for keyword in searchString.split(','):
								regexString += (r'\b' + keyword + r'\b.*')
							pattern = re.compile(regexString, re.IGNORECASE)
							for paragraph in [article['headline'], article['byline'], article['leadParagraph'], article['tailParagraph']]:
								if re.search(pattern, paragraph) is not None:
									articleSentenceList.append(paragraph)
						lineList = [articleCompanyCode, articleCompanyName, article['filePath'], article['_id'], article['date'], article['sourceName'], article['byline'], article['headline'] , '\t'.join(articleSentenceList)]
						encodedLineList = []
						for part in lineList:
							if isinstance(part, str) or isinstance(part, unicode):
								part = part.encode('utf-8').strip()
							encodedLineList.append(part)
						writer.writerow(encodedLineList)
					except Exception as e:
						print(e)

	def exportAllCitationBlock(self):
		quotePattern = re.compile(r'\"[^\"]+\"')

		citeWordPatternStringList = []
		for citeWord in self.citeWordList:
			citeWordPatternStringList.append((r'\b' + citeWord + r'\b'))

		citeWordPattern = re.compile(r'|'.join(citeWordPatternStringList), re.IGNORECASE)
		with open('export/allCiteSentence.csv', 'wb') as f:
			writer = csv.writer(f)
			attributeList = ['cotic', 'coname', 'filePath', 'accessNo', 'date', 'source', 'byline', 'headline', 'sentence', 'cite_content', 'cite_word', 'actor', 'organization']
			writer.writerow(attributeList)

			for i, article in enumerate(self.db.getAllArticle(510)):
				print(i)
				articleCompanyCode = article['filePath'].split('/')[-2]
				articleCompany = self.db.getCompanyByCode(articleCompanyCode)
				articleCompanyName = articleCompanyCode if articleCompany is None else articleCompany['name']
				articleLineListPart = [articleCompanyCode, articleCompanyName, article['filePath'], article['_id'], article['date'], article['sourceName'], article['byline'], article['headline']]

				for paragraph in [article['leadParagraph'], article['tailParagraph']]:
					quotedStringList = re.findall(quotePattern, paragraph)
					if quotedStringList and max([len(string.split()) for string in quotedStringList]) > 5:
						sentenceList = sent_tokenize(paragraph)
						for sentence in sentenceList:
							sentence = sentence.encode('utf-8')
							quotedStringList = re.findall(quotePattern, sentence)
							citeWordList = re.findall(citeWordPattern, ' '.join(getProcessedWordList(sentence, lemmatizeType=VERB)))
							if quotedStringList and max([len(string.split()) for string in quotedStringList]) > 5 and citeWordList:
								actorAndOrgList = self.processCiteSentence(sentence)
								if actorAndOrgList:
									lineList = articleLineListPart + [sentence, ' '.join(quotedStringList), ' '.join(citeWordList)] + actorAndOrgList
									encodedLineList = []
									for part in lineList:
										if isinstance(part, str) or isinstance(part, unicode):
											part = part.encode('utf-8').strip()
										encodedLineList.append(part)
									writer.writerow(encodedLineList)


	def processCiteSentence(self, sentence):
		#use name entity tagger
		actorList = []
		orgnizationList = []
		tagTupleList = self.NERTagger.tag(sentence.split())
		inQuoteFlag = False
		lastTag = ''
		wordList = []
		for i in range(len(tagTupleList)):
			if tagTupleList[i][0] == '\'\'' or tagTupleList[i][0] == '``' or tagTupleList[i][0] == '\"\"':
				inQuoteFlag = 1 - inQuoteFlag
				if not inQuoteFlag:
					del wordList[:]
			else:
				if not inQuoteFlag:
					if tagTupleList[i][1] != lastTag:
						if lastTag == 'PERSON':
							actorList.append(' '.join(wordList))
						elif lastTag == 'ORGANIZATION':
							orgnizationList.append(' '.join(wordList))
						wordList = [tagTupleList[i][0]]
						lastTag = tagTupleList[i][1]
					else:
						wordList.append(tagTupleList[i][0])
		if not actorList and not orgnizationList:
			return None
		else:
			return [', '.join(actorList), ', '.join(orgnizationList)]

if __name__ == '__main__':
	de = DataExporter()
	# de.exportSentenceAnalysis()
	# de.exportArticleAnalysis()
	de.exportAllCitationBlock()