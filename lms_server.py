import grpc
from concurrent import futures
import time
import lms_pb2
import lms_pb2_grpc

# Define hardcoded credentials for students and instructors
users = {
    "student": "student_pass",
    "instructor": "instructor_pass"
}

class LMSServiceServicer(lms_pb2_grpc.LMSServiceServicer):
    def __init__(self):
        self.sessions = {}  # Token to user mapping
        self.data_store = {}  # Store assignments and other data by type
        self.tutoring_channel = grpc.insecure_channel('localhost:50052')
        self.tutoring_stub = lms_pb2_grpc.TutoringServerStub(self.tutoring_channel)

    def Login(self, request, context):
        # Check if the username exists and the password matches
        if request.username in users and users[request.username] == request.password:
            token = f"token-{time.time()}"  # Generate a simple token
            self.sessions[token] = request.username
            return lms_pb2.LoginResponse(success=True, token=token)
        return lms_pb2.LoginResponse(success=False, token="")

    def Logout(self, request, context):
        if request.token in self.sessions:
            del self.sessions[request.token]
            return lms_pb2.StatusResponse(success=True, message="Logged out successfully")
        return lms_pb2.StatusResponse(success=False, message="Invalid token")

    def Post(self, request, context):
        if request.token in self.sessions:
            if request.type not in self.data_store:
                self.data_store[request.type] = []
            self.data_store[request.type].append(request.data)
            return lms_pb2.StatusResponse(success=True, message="Data posted successfully")
        return lms_pb2.StatusResponse(success=False, message="Invalid token")

    def Get(self, request, context):
        if request.token in self.sessions:
            data = self.data_store.get(request.type, [])
            data_items = [lms_pb2.DataItem(type_id=str(i), data=item) for i, item in enumerate(data)]
            return lms_pb2.GetResponse(data=data_items)
        return lms_pb2.GetResponse(data=[])
        

    def getLLMAnswer(self, request, context):
        # Forward query to the tutoring server
        tutoring_response = self.tutoring_stub.getLLMAnswer(
            lms_pb2.getLLMAnswerRequest(queryId=request.queryId, query=request.query))
        return tutoring_response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lms_pb2_grpc.add_LMSServiceServicer_to_server(LMSServiceServicer(), server)
    server.add_insecure_port('[::]:50051')  # LMS Server port
    server.start()
    print("LMS Server started on port 50051.")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()

# Source: ChatGPT