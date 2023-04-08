# Reddit-comment-generator

### Overview
This project is a Python script for scraping Reddit threads, collecting recent posts, and extracting information (the number of upvotes, the most popular comment and its upvotes). The script includes: 
- Tread scraper which collects recent 15000 posts.
- Text preprocessing steps (cleaning, spell check, tokenizing).
- Clustering and topic identification algorithms.
- Tensorflow GPT2 Comment generation feature to generate a comment based on the post text ( Hugging Face library).
- PyTorch GPT2 Comment generation feature (not trained).

### Dependencies
This project requires the following dependencies:

  praw
  bs4
  textblob
  nltk
  scipy
  wordcloud
  scikit-learn
  torch
  transformers
