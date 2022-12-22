from transformers import pipeline
from pymongo import MongoClient

pipe = pipeline('sentiment-analysis', model='emre/turkish-sentiment-analysis')
CONN_STR = 'mongodb+srv://crystalin:crystalin@crystalin.w93y0ww.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(CONN_STR, serverSelectionTimeoutMS=5000)

db = client['crystalin']  # select the database from cluster
collection = db['analyzed-data-collection']

documents = db['scraped-data-collection'].find({'comments.isAnalyzed': True})
for document in documents:
    product_name = document['productName']
    datas = document['comments']
    for data in datas:
        is_analyzed = data['isAnalyzed']
        scrape_date = data['scrapeDate']

        trendyol = data['trendyol']
        for comment in trendyol:
            id = comment['id']
            content = comment['content']
            analyze_result = pipe(content)[0]
            label = analyze_result['label']
            score = analyze_result['score']
            collection.insert_one({'_id': id, 'analyzeResult': {'sentiment': label, 'confidence': score}})
            print('trendyol insert')

        amazon = data['amazon']
        for comment in amazon:
            id = comment['id']
            content = comment['content']
            analyze_result = pipe(content)[0]
            label = analyze_result['label']
            score = analyze_result['score']
            collection.insert_one({'_id': id, 'analyzeResult': {'sentiment': label, 'confidence': score}})
            print('amazon insert')

        hepsiburada = data['hepsiburada']
        for comment in hepsiburada:
            id = comment['id']
            content = comment['content']
            analyze_result = pipe(content)[0]
            label = analyze_result['label']
            score = analyze_result['score']
            collection.insert_one({'_id': id, 'analyzeResult': {'sentiment': label, 'confidence': score}})
            print('hepsiburada insert')

        db['scraped-data-collection'].update_one({'productName': product_name, 'comments.scrapeDate': scrape_date},
                                                 {'$set': {'comments.$.isAnalyzed': True}})
        print('analyze result status updated')
        