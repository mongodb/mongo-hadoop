from pig_util import outputSchema
from nltk.tokenize.regexp import WordPunctTokenizer
from nltk.probability import FreqDist

notable_words = ['lawyer', 'nervous', 'lie']

@outputSchema('emails:bag{t:(FROM:chararray, TO:chararray, BODY:chararray)}')
def deserialize_receivers(FROM, TOs, BODY):
    return [ (FROM.strip(), TO.strip(), BODY) for TO in TOs.split(',') ]
    
@outputSchema('good_email:int')
def is_enron(email):
    if '@enron.com' in email:
        return 1
    else:
        return 0
        
@outputSchema('score:int')
def rate_email(body):
    words = WordPunctTokenizer().tokenize(body)
    word_freq = FreqDist([w.lower() for w in words])
    score = 0
    for word in notable_words:
        score += word_freq[word]
    return score
    
    
    





    from nltk.tokenize.regexp import WordPunctTokenizer
    from nltk.probability import FreqDist
    from nltk.stem.porter import PorterStemmer

    from numpy import std
    from numpy import array

    notable_words = ['lawyer', 'nervous', 'lie', 'bankrupt']

    @outputSchema('emails:bag{t:(FROM:chararray, TO:chararray, BODY:chararray)}')
    def deserialize_receivers(FROM, TOs, BODY):
        return [ (FROM.strip(), TO.strip(), BODY) for TO in TOs.split(',') ]

    @outputSchema('date:chararray')
    def time_bucket(date):
        tokens = date.split(' ')
        return tokens[2] + " " + tokens[3]
    @outputSchema('good_email:int')
    def is_enron(email):
        if '@enron.com' in email:
            return 1
        else:
            return 0

    @outputSchema('score:int')
    def rate_email(body):
        words = WordPunctTokenizer().tokenize(body)
        word_freq = FreqDist([w.lower() for w in words])
        score = 0
        for word in notable_words:
            score += word_freq[word]
        return score