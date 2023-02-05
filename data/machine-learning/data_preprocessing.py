from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from datetime import datetime


now = datetime.now()
# Initialize the stemmer
stemmer = PorterStemmer()


# Remove stopwords
stop_words = set(stopwords.words("english"))
reviews_df["review_text"] = reviews_df["review"].apply(lambda x: " ".join([word for word in word_tokenize(x) if word not in stop_words]) if pd.notnull(x) else '')
reviews_df["review_text"] = reviews_df["review"].apply(lambda x: " ".join([stemmer.stem(word) for word in word_tokenize(x)]) if pd.notnull(x) else '')
reviews_df["title_text"] = reviews_df["title"].apply(lambda x: " ".join([word for word in word_tokenize(x) if word not in stop_words]) if pd.notnull(x) else '')
reviews_df["title_text"] = reviews_df["title"].apply(lambda x: " ".join([stemmer.stem(word) for word in word_tokenize(x)]) if pd.notnull(x) else '')

