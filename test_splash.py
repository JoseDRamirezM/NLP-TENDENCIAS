import requests
from googletrans import Translator
#pip install googletrans==4.0.0-rc1 
import re
from bs4 import BeautifulSoup
from textblob import TextBlob
import pandas as pd

########################
#Dataframe

def add_dash(product_id):
    return product_id.replace("MCO", "MCO-")

def get_products_df():

    df = pd.read_csv('./products.csv')
    all_products_dirty = df['k_products']
    all_products = all_products_dirty.transform(add_dash)
    return all_products

###############################################

url = "https://articulo.mercadolibre.com.co/MCO-458337978"

splash_url = "http://localhost:8050/render.html"


def get_reviews(splash_url, target_url):

    r = requests.get(splash_url, 
                        params={'url':target_url, 'wait': 2})

    soup = BeautifulSoup(r.text, 'html.parser')

    mydivs = soup.find_all("p", {'class': "ui-review-capability-comments__comment__content", 'role':"presentation"})

    translator = Translator()

    translated = []

    for review in mydivs:
        translated_text = translator.translate(str(review.text), src="es")
        translated.append(translated_text.text)

    return(translated)


def normalize(text):
    # todo a minúsculas
    text = text.lower()

    # tildes y diacríticas
    text = re.sub('á', 'a', text)
    text = re.sub('é', 'e', text)
    text = re.sub('í', 'i', text)
    text = re.sub('ó', 'o', text)
    text = re.sub('ú', 'u', text)
    text = re.sub('ü', 'u', text)
    text = re.sub('ñ', 'n', text)

    return text

def clean(text):
    chars = r'[,;.:¡!¿?@#$%&[\](){}<>~=+\-*/|\\_^`"\']'
    
    # signos de puntuación
    text = re.sub(chars, ' ', text)
    
    # dígitos [0-9]
    text = re.sub('\d', ' ', text)

    return text

def preprocesar(text):
    text = clean(text)
    text = normalize(text)
    return text

def get_polarity(text):
    return TextBlob(text).sentiment.polarity

def get_subjectivity(text):
    return TextBlob(text).sentiment.subjectivity

reviews_list = get_reviews(splash_url, url)
reviews_df = pd.DataFrame(reviews_list, columns=['Review'])
#preprocesar
reviews_df = reviews_df['Review'].transform(preprocesar)
print(reviews_df)

polarity = reviews_df.apply(get_polarity)
subjectivity = reviews_df.apply(get_subjectivity)

result = pd.DataFrame({ 'Review': reviews_df,
                       'Polarity': polarity,
                        'Subjectivity': subjectivity })

print(result)


