import json
import pandas as pd
import requests
from config import url # url file with api key fed manually 

data = json.loads(requests.get(url).text)
df = pd.json_normalize(data, record_path=['orders'])
dfnew = df[["id", "app_id", "closed_at", "created_at", "currency", "email", "order_number", "processed_at", "source_name", "subtotal_price", "total_tax", "total_shipping_price_set.shop_money.amount",
            "total_shipping_price_set.shop_money.currency_code", "total_shipping_price_set.presentment_money.amount", "total_shipping_price_set.presentment_money.currency_code", "total_discounts", "total_price", "updated_at"]]

#below step is required because bigquery doesn't accepts '.' in column name
dfnew = dfnew.rename(columns={'total_shipping_price_set.shop_money.amount': 'total_shipping_price_set_shop_money_amount', 'total_shipping_price_set.shop_money.currency_code': 'total_shipping_price_set_shop_money_currency_code',
                              'total_shipping_price_set.presentment_money.amount': 'total_shipping_price_set_presentment_money_amount', 'total_shipping_price_set.presentment_money.currency_code': 'total_shipping_price_set_presentment_money_currency_code'})

result = dfnew.to_json(orient="table")
parsed = json.loads(result)
json_object = json.dumps(parsed)

# Writing to sample.json
with open("orders_edit.json", "w") as outfile:
    outfile.write(json_object)
