from transformers import AutoTokenizer, pipeline, AutoModelForSequenceClassification

# Fine-tune a BERT model on the reviews
tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
model = AutoModelForSequenceClassification.from_pretrained("bert-base-uncased")


# Initialize the sentiment classifier pipeline
sentiment_classifier = pipeline("sentiment-analysis", truncation=True, max_length=512)

# Get the sentiment predictions for the 'review_text' column
reviews_df["sentiment_review"] = reviews_df["review_text"].apply(lambda x: sentiment_classifier(x)[0]['label'])

# Get the sentiment predictions for the 'title_text' column
reviews_df["sentiment_title"] = reviews_df["title_text"].apply(lambda x: sentiment_classifier(x)[0]['label'])

# Initialize the sentiment classifier pipeline
sentiment_classifier = pipeline("sentiment-analysis", truncation=True, max_length=512)

# Get the sentiment predictions for the 'review_text' column
reviews_df["sentiment_review_score"] = reviews_df["review_text"].apply(lambda x: sentiment_classifier(x)[0]['score'])

# Get the sentiment predictions for the 'title_text' column
reviews_df["sentiment_title_score"] = reviews_df["title_text"].apply(lambda x: sentiment_classifier(x)[0]['score'])

reviews_df.to_csv(f'reviews_data_{now.month:02d}_{now.day:02d}.csv', index = False)