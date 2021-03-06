import re
import unicodedata

def findTokens(string, match):
    string_list = string.split()
    match_list = []
    for word in string_list:
        if match in word:
            match_list.append(word)
    return match_list

def normalizeString(s, char):
    s = s.lower()
    s = re.sub('[^0-9a-zA-Z]+', char, s)
    return s

def normalizeAscii(value):
    return unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('utf-8')