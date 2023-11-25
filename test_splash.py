import requests
from googletrans import Translator
#pip install googletrans==4.0.0-rc1 
import re
from bs4 import BeautifulSoup
from textblob import TextBlob
import pandas as pd
import traceback

#########
#DATABASE

from database import Database
database = Database()

def get_products():
    return database.get_products()

##########################
# Failure checks
def check_splash_conn(splash_url):
    r = requests.get(splash_url)
    if r.status_code == 200:
        return True
    return False


########################
# Raw products Dataframe

def add_dash(product_id):
    return product_id.replace("MCO", "MCO-")

def get_products_df():

    df = pd.read_csv('./products.csv')
    all_products_dirty = df['k_products']
    all_products = all_products_dirty.transform(add_dash)
    return all_products

###############################################
# Web scraping process

def get_reviews(splash_url, target_url):

    try:
        r = requests.get(splash_url, 
                            params={'url':target_url, 'wait': 2})
        print(r.status_code)

        soup = BeautifulSoup(r.text, 'html.parser')

        mydivs = soup.find_all("p", {'class': "ui-review-capability-comments__comment__content", 'role':"presentation"})

        translator = Translator()

        translated = []
        original = []

        if mydivs:
            for review in mydivs:
                clean_text = preprocesar(review.text)
                translated_text = translator.translate(str(clean_text), src="es")
                translated.append(translated_text.text)
                original.append(clean_text)

        return [translated, original]
    except:
        print("No se obtuvieron comentarios")


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

    return ' '.join(text.split())

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
    return '%.2f'%(TextBlob(text).sentiment.polarity)

def get_subjectivity(text):
    return '%.2f'%(TextBlob(text).sentiment.subjectivity)

################################

# Result Dataframe 

def get_score_word(score):
    if float(score) > 0.4:
        return 'buena'
    elif float(score) >= 0.3:
        return 'regular'
    return 'mala'
###############################

# Driver program
splash_url = "http://localhost:8050/"
try:
    if check_splash_conn(splash_url):
        # Obtener productos de la BD
        products = get_products()[0]
        last = products.index(('MCO1344037109',))
        print(last)
        products = products[last+1: ]
        #print(products)

        # Output DF
        #output = pd.DataFrame(columns=['product', 'reviews', 'score', 'score_word'])

        print(f"Hay {len(products)} productos registrados")
        for product in products:
                product_id = add_dash(product[0])
                url = "https://articulo.mercadolibre.com.co/{}".format(product_id)
                splash_url = "http://localhost:8050/render.html"
                print("Beginning process for: " + str(product))
                reviews_list = get_reviews(splash_url, url)
                # Verify reviews are retrieved
                if reviews_list and reviews_list[0]:
                    reviews_df = pd.DataFrame(reviews_list[0], columns=['Review'])
                    #preprocesar
                    reviews_df = reviews_df['Review'].transform(preprocesar)
                    polarity = reviews_df.apply(get_polarity)
                    #subjectivity = reviews_df.apply(get_subjectivity)

                    result = pd.DataFrame({ 'Review': reviews_list[1],
                                            'Polarity': polarity})
                                            #'Subjectivity': subjectivity })
                    #print(result)
                    # Datos para insertar, como las listas son simétricas es más rápido
                    # que iterar sobre el DataFrame
                    reviews_list = result['Review'].tolist()
                    polarity_list = result['Polarity'].tolist()

                    # Insertar datos en BD

                    # Test query
                    for x in range(0, len(reviews_list)):
                        comment_dict = {
                            'k_product' : product[0],
                            'nlp_score' : polarity_list[x],
                            'comment_text': reviews_list[x]
                        }
                        database.insert_comments(comment_dict)

#                 mean = '%.2f'%(result['Polarity'].mean())
#                 reviews = result['Review'].values.tolist()
#                 # Add new row to DF    
#                 output.loc[len(output.index)] = [product, [reviews], mean, get_score_word(mean)]
#                 print("Success")
                else:
                    # Manage empty reviews
                    #output.loc[len(output.index)] = [product, [[]], 0, 0]
                    print("Reviews were empty")
#             break
#             #print(output)
#         except Exception as e:
#             print("-------------------ERROR---------------------------------")
    #             print(e)
        
except Exception as e:
    print("-------------------WARNING---------------------------------")
    #traceback.print_exc()
    print(e)

# print(output)
# # Export final dataframe to csv
# output.to_csv('./results.csv', sep=',', encoding='utf-8')