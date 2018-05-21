import urllib2
import json
import time


from gensim.models import KeyedVectors
word_vectors = KeyedVectors.load_word2vec_format('../data/GoogleNews-vectors-negative300.bin', binary=True)
count = 0
url = 'http://localhost:8000/v1/insert'
for w in word_vectors.vocab:
    postdata = {
        'feature': word_vectors[w].tolist(),
        'timestamp': 0,
        'label': w
    }
    req = urllib2.Request(url, json.dumps(postdata), headers={'Content-type': 'application/json', 'Accept': 'application/json'})
    response = urllib2.urlopen(req)
    print(w)
    count += 1
    if (count > 100):
        break

# url = 'http://localhost:8000/v1/insert'
# postdata = {
#    'feature': [0.5, 0.1, 0.2],
#    'timestamp': 2312323,
#    'label': 'example1'
# }

# req = urllib2.Request(url, json.dumps(postdata), headers={'Content-type': 'application/json', 'Accept': 'application/json'})
# response = urllib2.urlopen(req)
# the_page = response.read()
# print(the_page)

# time.sleep(5)

postdata = {
    'feature': [0.5, 0.1, 0.2],
    'k': 10,
    'timestamp': 2312323,
    'timeout': 1212
}
url = 'http://localhost:8000/v1/search'
req = urllib2.Request(url, json.dumps(postdata), headers={'Content-type': 'application/json', 'Accept': 'application/json'})
response = urllib2.urlopen(req)
data = json.load(response)
for f in data.points:
    print(f["label"])

# curl --header "Content-Type: application/json" \
#  --request POST \
#  --data '{"feature": [0.5, 0.1, 0.2],"timestamp": 2312323, "label": "example1"}' \
#  http://localhost:8000/v1/insert
