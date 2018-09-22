import grpc

from veriservice import veriservice_pb2 as pb
from veriservice import veriservice_pb2_grpc as pb_grpc

class VeriClient:
    def __init__(self, service):
        self.service = service # eg.: 'localhost:50051'
        self.channel = grpc.insecure_channel(self.service)
        self.stub = pb_grpc.VeriServiceStub(self.channel)

    def insert(self, feature, label, grouplabel, timestamp):
        request = pb.InsertionRequest(timestamp = timestamp, label = label, grouplabel = grouplabel, feature = feature)
        response = self.stub.Insert(request)
        return response

    def getKnn(self, feature, k=10, id='', timestamp=0, timeout=1000):
        request = pb.KnnRequest(id=id, timestamp=timestamp, timeout=timeout, k=k, feature=feature)
        response = self.stub.GetKnn(request)
        return response

    def get(self, label):
        request = pb.GetRequest(label = label)
        response = self.stub.Get(request)
        return response

    def getLocalData(self):
        request = pb.GetLocalDataRequest()
        response = self.stub.GetLocalData(request)
        return response

class DemoVeriClientWithData:
    def __init__(self, service):
        self.client = VeriClient(service) # eg.: 'localhost:50051'
        self.data = [
            {
                'label': 'a',
                'feature': [0.5, 0.1, 0.2]
            },
            {
                'label': 'b',
                'feature': [0.5, 0.1, 0.3]
            },
            {
                'label': 'c',
                'feature': [0.5, 0.1, 1.4]
            },
             ]

    def runExample(self):
        print('inserting data')
        for d in self.data:
            self.client.insert(d['feature'], d['label'], d['label'], 0)
        print('get back data')
        for d in self.data:
            print(self.client.get(d['label']).feature[:3])
        print('do a knn search')
        print(self.client.getKnn([0.1, 0.1, 0.1]))
