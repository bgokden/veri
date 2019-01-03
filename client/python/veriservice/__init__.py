import grpc
import time
import random

from veriservice import veriservice_pb2 as pb
from veriservice import veriservice_pb2_grpc as pb_grpc

__version__ = "0.0.17"

class GrpcClientWrapper:
    def __init__(self, service, client):
        self.service = service
        self.client = client

    def get_service(self):
        return self.service

    def get_client(self):
        return self.client

class VeriClient:
    def __init__(self, services):
        self.services = services.split(",") # eg.: 'localhost:50051, localhost2:50051'
        self.clients = {}

    def __get_client(self):
        service = random.choice(self.services)
        if service in self.clients:
            return self.clients[service]
        channel = grpc.insecure_channel(service)
        self.clients[service] = GrpcClientWrapper(service, pb_grpc.VeriServiceStub(channel))
        return self.clients[service]

    def __refresh_client(self, service):
        channel = grpc.insecure_channel(service)
        self.clients[service] = GrpcClientWrapper(service, pb_grpc.VeriServiceStub(channel))
        time.sleep(5)


    def insert(self,
                feature,
                label,
                grouplabel = '',
                timestamp = 0,
                sequencelengthone = -1,
                sequencelengthtwo = -1,
                sequencedimone = -1,
                sequencedimtwo = -1,
                retry = 5):
        request = pb.InsertionRequest(timestamp = timestamp,
                                        label = label,
                                        grouplabel = grouplabel,
                                        feature = feature,
                                        sequencelengthone = sequencelengthone,
                                        sequencelengthtwo= sequencelengthtwo,
                                        sequencedimone = sequencedimone,
                                        sequencedimtwo = sequencedimtwo)
        response = None
        while retry >= 0:
            client_wrapper = None
            try:
                client_wrapper = self.__get_client()
                response = client_wrapper.get_client().Insert(request)
                if response.code == 0:
                    return response
            except grpc.RpcError as e: # there should be connection problem
                if client_wrapper is not None:
                    self.__refresh_client(client_wrapper.get_service())
            except Exception as e:
                print(e)
                time.sleep(5)
            retry -= 1
        return response

    def getKnn(self, feature, k=10, id='', timestamp=0, timeout=1000, retry = 5):
        request = pb.KnnRequest(id=id, timestamp=timestamp, timeout=timeout, k=k, feature=feature)
        response = None
        while retry >= 0:
            client_wrapper = None
            try:
                client_wrapper = self.__get_client()
                response = client_wrapper.get_client().GetKnn(request)
                return response
            except grpc.RpcError as e: # there should be connection problem
                if client_wrapper is not None:
                    self.__refresh_client(client_wrapper.get_service())
            except Exception as e:
                print(e)
                time.sleep(5)
            retry -= 1
        return response

    def getLocalData(self, retry = 5):
        request = pb.GetLocalDataRequest()
        response = None
        while retry >= 0:
            client_wrapper = None
            try:
                client_wrapper = self.__get_client()
                response = client_wrapper.get_client().GetLocalData(request)
                return response
            except grpc.RpcError as e: # there should be connection problem
                if client_wrapper is not None:
                    self.__refresh_client(client_wrapper.get_service())
            except Exception as e:
                print(e)
                time.sleep(5)
            retry -= 1
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
        print('do a knn search')
        print(self.client.getKnn([0.1, 0.1, 0.1]))
