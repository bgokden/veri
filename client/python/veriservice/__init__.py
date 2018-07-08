import grpc
import veriservice.veriservice_pb2 as pb
import veriservice.veriservice_pb2_grpc as pb_grpc

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
