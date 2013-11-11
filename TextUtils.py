"""
Created on 2013-5-8
@author: Bobi Pu, bobi.pu@usc.edu
"""
import re
from Settings import *
from nltk.stem.wordnet import WordNetLemmatizer
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
		elif isStringContainWordInList(string, getWordList(WORD_INVALID_SENTENCE_FILTER)):
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

def getWordListFilePath(wordType):
	if wordType == WORD_INVALID_SENTENCE_FILTER:
		return 'word/validSentenceFilterWord.csv'
	elif wordType == WORD_FILTER:
		return 'word/filterWord.csv'
	elif wordType == WORD_CITE:
		return 'word/citeWord.csv'
	elif wordType == WORD_CAUSE_EX:
		return 'word/causality_ext.csv'
	elif wordType == WORD_CAUSE_IN:
		return 'word/causality_int.csv'
	elif wordType == WORD_CONTROL_LOW:
		return 'word/controlability_low.csv'
	elif wordType == WORD_CONTROL_HIGH:
		return 'word/controlability_high.csv'
	elif wordType == MCD_POS:
		return 'word/LoughranMcDonald_Positive.csv'
	elif wordType == MCD_NEG:
		return 'word/LoughranMcDonald_Negative.csv'
	elif wordType == MCD_UNCERTAIN:
		return 'word/LoughranMcDonald_Uncertainty.csv'

def getWordList(wordType):
	with open(getWordListFilePath(wordType)) as f:
		return [word.strip().lower() for word in f.readlines()]

def getWordDictWithWordList(wordList):
	return dict(zip(wordList, [0] * len(wordList)))

def getWordDict(wordType):
	wordList = getWordList(wordType)
	return getWordDictWithWordList(wordList)

def lemmatize(word):
	lemmatizedWord = lemmatizer.lemmatize(word, NOUN)
	if lemmatizedWord != word:
		return lemmatizedWord
	lemmatizedWord = lemmatizer.lemmatize(word, VERB)
	if lemmatizedWord != word:
		return lemmatizedWord
	lemmatizedWord = lemmatizer.lemmatize(word, ADJ)
	if lemmatizedWord != word:
		return lemmatizedWord
	return lemmatizer.lemmatize(word, ADV)


def sentenceToWordList(sentence, filterWordDict=None):
	#use this to extract keyword
	if filterWordDict is not None:
		wordList = [lemmatize(word.lower().strip()) for word in sentence.split() if unicode.isalnum(word)]
		return [word for word in wordList if word not in filterWordDict]
	else:
		return [lemmatize(word.lower().strip()) for word in sentence.split() if unicode.isalnum(word)]

def getWordRegexPattern(wordType):
	#check the word is unigram or bigram, then use different pattern paradigm
	wordList = getWordList(wordType)
	wordPatternStringList = []
	for wordString in wordList:
		#patternString = r'\b' + (wordString.split()[0] + r'( [\w\d]+)* ') + wordString.split()[1] + r'\b'
		wordPatternString = wordString if 1 == len(wordString.split()) else ' '.join(wordString.split())
		wordPatternString = r'\b' + wordPatternString + r'\b'
		wordPatternStringList.append(wordPatternString)
	patternString = r'|'.join(wordPatternStringList)
	pattern = re.compile(patternString, re.IGNORECASE)
	return pattern


def getMatchWordListFromPattern(text, pattern, filterWordDict):
	#filter and lemmatize the input text
	text = ' '.join(sentenceToWordList(text, filterWordDict))
	return pattern.findall(text)

def getQuotedString(string):
	matches = findall(r'"(.+?)"',string)
	return ', '.join(matches)

def getStringSurroundWordInDistance(string, word, distance):
	wordList = sentenceToWordList(string, defaulFilterWordDict)
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

lemmatizer = WordNetLemmatizer()
defaulFilterWordDict = getWordDict(WORD_FILTER)