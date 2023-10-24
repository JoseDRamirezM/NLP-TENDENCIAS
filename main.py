import pandas as pd

def add_dash(product_id):
    return product_id.replace("MCO", "MCO-")

df = pd.read_csv('./products.csv')

all_products_dirty = df['k_products']

all_products = all_products_dirty.transform(add_dash)

print(all_products)


