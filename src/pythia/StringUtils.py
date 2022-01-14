def findTokens(string, match):
    string_list = string.split()
    match_list = []
    for word in string_list:
        if match in word:
            match_list.append(word)
    return match_list