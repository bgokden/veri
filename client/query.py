import urllib2
import json
import time


from gensim.models import KeyedVectors
word_vectors = KeyedVectors.load_word2vec_format('../data/GoogleNews-vectors-negative300.bin', binary=True)


def query(vectors, word):
    start = time.clock()
    vec = vectors[word].tolist()
    end = time.clock()
    print("time1: " + str(end-start))
    postdata = {
        'feature': vec,
        'k': 10,
        'timestamp': 0,
        'timeout': 1000
    }
    url = 'http://localhost:8000/v1/search'
    start = time.clock()
    req = urllib2.Request(url, json.dumps(postdata), headers={'Content-type': 'application/json', 'Accept': 'application/json'})
    response = urllib2.urlopen(req)
    data = json.load(response)
    end = time.clock()
    print("time2: " + str(end-start))
    for f in data["points"]:
        print(f["label"])


def query2(vectors, word):
    start = time.clock()
    vec = vectors[word].tolist()
    end = time.clock()
    time1 = end-start
    start = time.clock()
    result = vectors.most_similar(positive=[word])
    # .similar_by_vector(vector=vec, topn=10)
    end = time.clock()
    print("time2: " + str(end-start-time1))
    print(result)

# curl --header "Content-Type: application/json" \
#  --request POST \
#  --data '{"feature": [0.5, 0.1, 0.2],"timestamp": 2312323, "label": "example1"}' \
#  http://localhost:8000/v1/insert
