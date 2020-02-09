from datetime import date, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import regex
from langdetect import detect
import requests
import re
import urllib.parse
from urllib.request import urlopen
from bs4 import BeautifulSoup
from func_timeout import func_timeout
import statistics
import math
import time
from googletrans import Translator

class NewsArticles:
    def get_news_articles(name, ID, newsapi, cursor, connection):
        company_name = str(name)
        ID = str(ID)
        yesterday = date.today() - timedelta(2)
        # Adding more search terms from the database to retrieve more relevant articles
        cursor.execute(
            "SELECT AltSearchTerm from CWAlternativeSearchTerm WHERE lCompanyID = " + ID + ' AND IsEnabled = 1')
        search_terms = cursor.fetchall()
        complete_search_term = company_name
        for search_term in search_terms:
            search_term = str(search_term)
            search_term = urllib.parse.quote(
                search_term[search_term.find('\'') + 1:search_term.rfind('\'', search_term.find('\'') + 1)])
            complete_search_term += ' OR ' + search_term
        news_results = newsapi.get_everything(q=complete_search_term, page_size=100,
                                              from_param=yesterday.strftime('%Y-%m-%d'))
        print('Fetching news on: ' + company_name + ' Articles found: ' + str(news_results["totalResults"]))
        analyzer = SentimentIntensityAnalyzer()  # Instantiating objects for sentiment and translation
        translator = Translator()
        for page in range(math.ceil(float(news_results["totalResults"]) / float(100))):  # Go through each page
            for article in news_results["articles"]:
                # Article may have already been stored from a previous time
                cursor.execute(
                    "SELECT COUNT(*) from CWMessages WHERE Source = 'News' AND SourceId = '" + article["url"] +
                    "' AND lCompanyID = " + ID)
                existing_results = cursor.fetchall()
                x = 0
                for x in existing_results:
                    x = x[0]
                try:
                    request = requests.get(article['url'],
                                           headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) App'
                                                    'leWebKit/537.36 ''(KHTML, like Gecko) Chrome/39.0.2'
                                                    '171.95 Safari/537.36'}, allow_redirects=False)
                    decoded_request = request.content.decode('latin-1')
                except:
                    # Spoofing different user-agents to fool the website into thinking that I am not a bot
                    try:
                        time.sleep(1)  # A website will block multiple requests in a short time period
                        request = requests.get(article['url'],
                                               headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'
                                                                      'AppleWebKit/537.11 (KHTML, like Gecko)'
                                                                      'Chrome/23.0.1271.64 Safari/537.11',
                                                        'Accept': 'text/html,application/xhtml+xml,'
                                                                  'application/xml;q=0.9,*/*;q=0.8'},
                                               allow_redirects=False)
                        decoded_request = request.content.decode('latin-1')
                    except:
                        try:
                            time.sleep(1)
                            request = requests.get(article['url'], headers={
                                'Accept-Encoding': 'gzip, deflate, sdch', 'Accept-Language': 'en-US,en;q=0.8',
                                'Upgrade-Insecure-Requests': '1',
                                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, '
                                              'like Gecko) Chrome/56.0.2924.87 Safari/537.36',
                                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q='
                                          '0.8', 'Cache-Control': 'max-age=0', 'Connection': 'keep-alive'},
                                                   allow_redirects=False)
                            decoded_request = request.content.decode('latin-1')
                        except:
                            decoded_request = str(article['content'])
                # If the article doesn't exist in the dB, push data through if the article is relevant
                if int(x) == 0:
                    translated = 0
                    try:
                        lang = detect(article['title'] + article['description'])
                    except:
                        lang = 'Unknown'
                    if lang != 'en' and lang != 'Unknown':
                        translated = 1
                        # Article must be in json format to be translated
                        request = requests.get(article['url'],
                                               headers={
                                                   'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) App'
                                                                 'leWebKit/537.36 ''(KHTML, like Gecko) Chrome/39.0.2'
                                                                 '171.95 Safari/537.36'}, allow_redirects=False).json()
                        decoded_request = request.content.decode('latin-1')
                    message = 'HEADLINE: ' + str(article['title']) + ' - ' + str(article['source']['name'])
                    # Get number of mentions
                    cursor.execute("SELECT MentionTerm FROM CWMentionTerms WHERE lCompanyID = " + ID)
                    mention_terms = cursor.fetchall()
                    mentions = 0
                    in_headline = 0
                    for mention in mention_terms:
                        mention = str(mention)
                        mention = mention[mention.find('\'') + 1:mention.rfind('\'', mention.find('\'') + 1)]
                        mentions += len(re.findall(mention, decoded_request, flags=re.IGNORECASE))
                        if len(re.findall(mention, message, flags=re.IGNORECASE)) > 0:
                            in_headline = 1
                        else:
                            in_headline = 0
                    # Filter out articles that do not match the mention requirement
                    cursor.execute("SELECT MentionCriteria FROM CWMentionCriteria WHERE lCompanyID = " + ID)
                    mention_criteria = cursor.fetchall()
                    men_criteria = 1
                    for criteria in mention_criteria:
                        criteria = str(criteria)
                        try:
                            men_criteria = int(criteria[1])  # Mention criteria is a single digit
                        except:
                            men_criteria = int(criteria[1:2])  # Double digit
                    # Filter each article through the database filter terms
                    cursor.execute("SELECT FilterTerm FROM CWFilterTerms WHERE ISearchTermID = " + ID)
                    filter_terms = cursor.fetchall()
                    for filter_word in filter_terms:
                        filter_word = str(filter_word)
                        filter_word = filter_word[filter_word.find('\'') + 1:
                                                  filter_word.rfind('\'', filter_word.find('\'') + 1)]
                        if len(re.findall(filter_word, decoded_request, flags=re.IGNORECASE)) > 0 or \
                                len(re.findall(filter_word, message, flags=re.IGNORECASE)) > 0:
                            mentions = 0
                            in_headline = 0
                    if mentions >= men_criteria and in_headline == 0 \
                            or in_headline == 1:
                        # Published Time
                        published_datetime = str(article['publishedAt'])
                        date_time = published_datetime[:published_datetime.find('T')] + \
                            ' ' + published_datetime[published_datetime.find('T') +
                                                     1:published_datetime.find('z')] + '.000'
                        # Filter out banned words
                        filtered_message = message
                        cursor.execute("SELECT Word from CWFilterWords")
                        banned_array = cursor.fetchall()
                        for banned_word in banned_array:
                            banned_word = str(banned_word)
                            banned_word = banned_word[
                                          banned_word.find('\'') + 1:banned_word.rfind(
                                              '\'', banned_word.find('\'') + 1)]
                            bad_word_beg = message.find(banned_word)
                            if bad_word_beg > -1:
                                if message[bad_word_beg - 1:bad_word_beg].isspace() and \
                                        message[
                                        bad_word_beg + len(banned_word):bad_word_beg + len(banned_word) + 1].isspace():
                                    filtered_message = regex.sub(banned_word[0], len(banned_word[0]) * '*',
                                                                 filtered_message, flags=regex.IGNORECASE)
                        # Score
                        text = ''
                        # This is an array of tags that may contain the company name
                        tag_terms = ['a', 'link', 'options', 'desc', 'title', 'h1', 'h2', 'h3', 'h4', 'h5', 'span',
                                     'rss', 'ul', 'strong', 'script', 'style', 'li', 'div', 'span', 'option', 'b',
                                     'br', 'p', 'i']
                        # The first attempt with urlopen is the urlopen object
                        try:
                            #  When the duration of urlopen exceeds four seconds, raise an exception
                            html = func_timeout(4.0, article['url'].read(), args=None)
                            body_text = func_timeout(4.0, BeautifulSoup(urlopen(html).read(), features="html.parser"),
                                                     args=None)
                            # Delete the tags, and its contents, if the company name is not contained within the tag
                            for item in tag_terms:
                                for tag in body_text.find_all(item):
                                    orig_tag = str(tag)
                                    if orig_tag.find(company_name) > 0:
                                        continue
                                    else:
                                        tag.decompose()
                            for script in body_text(["script", "style"]):
                                script.extract()
                            text += body_text.get_text()  # Get the text
                        except:
                            # The second attempt with urlopen is now an array
                            try:
                                # Instead of reading the url, the decoded request can be sent to BeautifulSoup
                                body_text = func_timeout(4.0, BeautifulSoup(decoded_request, features="html.parser"),
                                                         args=None)
                                for item in tag_terms:
                                    for tag in body_text[0].find_all(item):
                                        orig_tag = str(tag)
                                        if orig_tag.find(company_name) > 0:
                                            continue
                                        else:
                                            tag.decompose()
                                for script in body_text[0](["script", "style"]):
                                    script.extract()
                                text += body_text[0].get_text()
                            except:
                                text = str(article['content'])
                        # Text must be decoded to json format for translation
                        if lang != 'en':
                            if text != '':
                                text = translator.translate(str(text))
                        #  Remove beginning or trailing end spaces
                        try:
                            lines = (line.strip() for line in text.splitlines())
                            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                            text = '\n'.join(chunk for chunk in chunks if chunk)  # Joins all newlines together into one
                        except:
                            continue
                        cursor.execute("SELECT MentionTerm FROM CWMentionTerms WHERE lCompanyID = " + ID)
                        mention_terms = cursor.fetchall()
                        compound_scores = []
                        positive_scores = []
                        negative_scores = []
                        neutral_scores = []
                        find_word = 1
                        for mention in mention_terms:
                            mention = str(mention)
                            mention = mention[mention.find('\'') + 1:mention.rfind('\'', mention.find('\'') + 1)]
                            while find_word >= 0:
                                # Find the index of the search term
                                find_word = text.find(mention)
                                if find_word == -1:
                                    find_word = text.find(mention.upper())
                                    if find_word == -1:
                                        find_word = text.find(mention.capitalize())
                                # The word exists
                                while find_word >= 0:
                                    #  These are the indexes of the beginning of the next sentence
                                    find_next_period = text.find('.', find_word)
                                    find_next_exclamation = text.find('!', find_word)
                                    find_next_question = text.find('?', find_word)
                                    original = find_next_period
                                    try:
                                        # Do not end a sentence wherever acronyms are used
                                        while text[find_next_period + 1].isdigit() or \
                                                text[find_next_period - 2:find_next_period] == "Mr" or \
                                                text[find_next_period - 2:find_next_period] == "Ms" or \
                                                text[find_next_period - 3:find_next_period] == "Mrs" or \
                                                text[find_next_period - 2:find_next_period] == "Dr" or \
                                                text[find_next_period + 1] == "," or \
                                                text[find_next_period - 4:find_next_period] == "Corp" or \
                                                text[find_next_period - 3:find_next_period] == "Inc" or \
                                                text[find_next_period - 3:find_next_period] == "Ltd" or \
                                                text[find_next_period - 2:find_next_period] == "Co":
                                            find_next_period = text.find('.', find_next_period + 1)
                                            find_next_exclamation = text.find('!', find_next_period + 1)
                                            find_next_question = text.find('?', find_next_period + 1)
                                    except:
                                        find_next_period = text.find('.', find_word)
                                        find_next_exclamation = text.find('!', find_word)
                                        find_next_question = text.find('?', find_word)
                                    if text.rfind('.', 0, find_next_period - 1) == -1:
                                        sentence = text[0:find_next_period]  # Sentence becomes the very start
                                        text = text[find_next_period:]
                                    elif find_next_exclamation < find_next_period and find_next_exclamation < \
                                            find_next_question and find_next_exclamation != find_next_question != -1:
                                        sentence = text[text.rfind('.', 0, find_next_exclamation - 1):find_next_exclamation]
                                        text = text[find_next_exclamation:]
                                    elif find_next_question < find_next_period and find_next_question < \
                                            find_next_exclamation and find_next_question != find_next_exclamation != -1:
                                        sentence = text[text.rfind('.', 0, find_next_question - 1):find_next_question]
                                        text = text[find_next_question:]
                                    else:
                                        sentence = text[text.rfind('.', 0,
                                                                   original - 1):find_next_period]
                                        text = text[find_next_period:]
                                    sentence.strip()  # Remove trailing/beginning whitespaces
                                    if sentence.startswith('.') or sentence.startswith('!') or sentence.startswith('?'):
                                        sentence = sentence[1:]
                                    vs = str(analyzer.polarity_scores(sentence))
                                    #  Getting the compound score:
                                    compound_scores.append(
                                        float(vs[vs.find('compound') + 11:vs.find(',', vs.find('compound'))]))
                                    #  Getting the negative scores
                                    negative_scores.append(float(vs[vs.find('neg') + 6:vs.find(',', vs.find('neg'))]))
                                    #  Getting the positive score:
                                    positive_scores.append(float(vs[vs.find('pos') + 6:vs.find(',', vs.find('pos'))]))
                                    #  Getting the neutral score:
                                    neutral_scores.append(float(vs[vs.find('neu') + 6:vs.find(',', vs.find('neu'))]))
                                    find_word = text.find(mention)  # On next iteration
                        try:
                            average_score = statistics.mean(compound_scores)
                            negative = statistics.mean(negative_scores)
                            positive = statistics.mean(positive_scores)
                            neutral = statistics.mean(neutral_scores)
                        except:
                            average_score = negative = positive = neutral = 0
                        # Weighted Compound Score
                        weighted_compound = (1 + 0.005 * in_headline + 0.001 * mentions) * average_score
                        # Keep score between 1 and -1
                        if weighted_compound > 1:
                            weighted_compound = 1
                        elif weighted_compound < -1:
                            weighted_compound = -1
                        # Collate values
                        values = (
                            message, filtered_message, negative, neutral, positive, average_score, article['url'],
                            date_time,
                            'news', ID, translated, weighted_compound, article['url'], lang, mentions, in_headline
                        )
                        # SQL statement to store in dB
                        SQLCommand = (
                            "INSERT INTO CWMessages(UnfilteredMessage, Message, negative, neutral, positive, compound, "
                            "SourceId, DatePosted, Source, lCompanyID, Translated, WeightedCompound, SourceURL, "
                            "MessageLanguage, MessageMentions, InHeadline)"
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                        )
                        try:
                            cursor.execute(SQLCommand, values)
                            connection.commit()
                        except:
                            # pyodbc.DataError: ('22001', '[22001] [Microsoft][ODBC SQL Server Driver]"
                            #                                   "[SQL Server]String or binary data would be truncated."
                            print("Unable to store article.")
                # Navigate to the next page
                if page < math.ceil(float(news_results["totalResults"]) / float(100)):
                    news_results = newsapi.get_everything(q=complete_search_term, page_size=100, page=page + 1,
                                                          from_param=yesterday.strftime('%Y-%m-%d'))