"""
Created on 2013-5-8
@author: Bobi Pu, bobi.pu@usc.edu
"""
import re

from Settings import *
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from math import log10
from collections import Counter
from copy import deepcopy
from re import findall
import itertools

def isValidSentence(string):
		if string.find('=') != -1:
			return False
		elif getStringWordCount(string) > PFM_SENTENCE_WORD_COUNT_HIGH_LIMIT or getStringWordCount(string) < PFM_SENTENCE_WORD_COUNT_LOW_LIMIT:
			return False
		elif getWordFrequencyInString(string, '$') > PFM_SENTENCE_DOLLAR_LIMIT:
			return False
		elif getWordFrequencyInString(string, '%') > PFM_SENTENCE_PERCENT_LIMIT:
			return False
		elif getWordFrequencyInString(string, '-') > PFM_SENTENCE_HYPHEN_LIMIT:
			return False
		elif isStringContainWordInList(string, getWordList(FILTER_PFM)):
			return False
		else:
			return True

def getWordFrequencyInString(string, word):
	return len(string.split(word)) - 1

def getStringWordCount(string):
	return len(string.split())

def isStringContainWordInList(string, wordList):
	for word in wordList:
		if string.find(word) != -1:
			return True
	return False

def isASCII(string):
	try:
		string.decode('ascii')
		return True
	except:
		return False

def getWordListFilePath(wordType):
	if wordType == WORD_POS:
		return 'word/posWord.txt'
	elif wordType == WORD_NEG:
		return 'word/negWord.txt'
	elif wordType == WORD_PFM:
		return 'word/pfmWord.txt'
	elif wordType == ATRB_EX:
		return 'word/exWord.txt'
	elif wordType == ATRB_IN:
		return 'word/inWord.txt'
	elif wordType == FILTER_WORD:
		return 'word/filterWord.txt'
	elif wordType == FILTER_PFM:
		return 'word/pfmFilterWord.txt'
	elif wordType == CITE_WORD:
		return 'word/citeWord.txt'

def getWordList(wordType):
	with open(getWordListFilePath(wordType)) as f:
		return [word.strip().lower() for word in f.readlines()]

def getWordDictWithWordList(wordList):
	return dict(zip(wordList, [0] * len(wordList)))

def getWordDict(wordType):
	wordList = getWordList(wordType)
	return getWordDictWithWordList(wordList)

def countWord(wordList, wordDict):
	for word in wordList:
		wordDict[word] += 1
	return wordDict

def getWordCountList(wordList, wordDict):
	wordCountList = []
	for word in wordList:
		wordCountList.append(wordDict[word])
	return wordCountList

def getKeyFromWordType(wordType):
	if wordType == WORD_POS:
		return 'pos'
	elif wordType == WORD_NEG:
		return 'neg'
	elif wordType == WORD_PFM:
		return 'pfm'
	elif wordType == ATRB_NO:
		return 'no'
	elif wordType == ATRB_IN:
		return 'in'
	elif wordType == ATRB_EX:
		return 'ex'
	elif wordType == CITE_WORD:
		return 'cite'
	else:
		raise Exception('invalid wordType value')

def getLemmatizer():
	if 'lemmatizer' not in globals():
		global lemmatizer
		lemmatizer = WordNetLemmatizer()
		return lemmatizer
	else:
		return lemmatizer

def getProcessedWordList(string, lemmatizeType=NOUN, isFilterWord=True):
	if 'lemmatizer' not in globals():
		global lemmatizer
		lemmatizer = WordNetLemmatizer()
	if 'filterWordDict' not in globals():
		global filterWordDict
		filterWordDict = getWordDict(FILTER_WORD)

	wordList = []
	for word in word_tokenize(string):
		word = lemmatizer.lemmatize(word.strip().lower(), lemmatizeType)
		if word.isalpha() and (not (isFilterWord and word in filterWordDict)) and len(word) > 1:
			wordList.append(word)
	return wordList

def getProcessedSentenceList(sentences, lemmatizeType=NOUN):
	return [' '.join(getProcessedWordList(sentence['content'], lemmatizeType)) for sentence in sentences]

def getTFIDFMatrix(sentences, wordTable):
	matrix = []
	sentences_2 = deepcopy(sentences)
	totalSentenceNumber = sentences.count()
	sentenceCountWithWordList = [0] * len(wordTable)
	for sentence in sentences:
		wordList, sentenceWordTable = getProcessedWordList(sentence['content']), {}
		for word in wordList:
			if word not in sentenceWordTable:
				sentenceCountWithWordList[wordTable[word]] += 1
				sentenceWordTable[word] = True
	for sentence in sentences_2:
		wordList, TFIDFList = getProcessedWordList(sentence['content']), [0] * len(wordTable)
		wordCountDict = Counter(wordList)
		for word in wordList:
			TFIDFList[wordTable[word]] = computTFIDF(wordCountDict[word], len(wordList), totalSentenceNumber, sentenceCountWithWordList[wordTable[word]])
		matrix.append(TFIDFList)
	return matrix

def computTFIDF(wordCountInSentence, sentenceWordNum, sentenceNum, sentenceNumberWithWord):
	tf = float(wordCountInSentence) / sentenceWordNum
	idf = log10(float(sentenceNum) / (sentenceNumberWithWord + 1))
	return tf * idf

def sumMatchingElememt(matrix):
	outputList = []
	for i in range(len(matrix[0])):
		outputList.append(sum([vector[i] for vector in matrix]))
	return outputList

def getAtrbWordList():
	return  getWordList(ATRB_IN) + getWordList(ATRB_EX)

def getAtrbWordDict():
	atrbWordList = getAtrbWordList()
	return dict(zip(atrbWordList, [0] * len(atrbWordList)))

def getQuotedString(string):
	matches = findall(r'"(.+?)"',string)
	return ', '.join(matches)

def getStringSurroundWordInDistance(string, word, distance):
	wordList = getProcessedWordList(string, NOUN)
	outputString = ''
	try:
		wordIndex = wordList.index(word)
		startIndex = 0 if wordIndex < distance else wordIndex - distance
		endIndex = len(wordList) if (wordIndex + distance) >= len(wordList) else wordIndex + distance
		outputString = ' '.join(wordList[startIndex : endIndex])
	except:
		pass
	return outputString

def getPatternByKeywordSearchString(searchString):
	if searchString is None or len(searchString) == 0:
		return None
	else:
		if searchString.find(',') != -1:
			keywordList = [keyword.strip() for keyword in searchString.split(',')]
			permutationRegexStringList = []
			for keywordTuple in itertools.permutations(keywordList):
				tupleRegexString = r'\b' + r'\b[^\.]*\b'.join(keywordTuple) + r'\b'
				permutationRegexStringList.append(tupleRegexString)
			regexString = r'|'.join(permutationRegexStringList)
		elif searchString.find(r'|') != -1:
			keywordList = [keyword.strip() for keyword in searchString.split('|')]
			regexString = r'\b(' + '|'.join(keywordList) + r')\b'
		else:
			regexString = r'\b' + searchString + r'\b'

		return re.compile(regexString, re.IGNORECASE)