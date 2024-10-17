import grpc
import lms_pb2
import lms_pb2_grpc

def login(stub):
    username = input("Enter username: ")
    password = input("Enter password: ")
    
    response = stub.Login(lms_pb2.LoginRequest(username=username, password=password))
    
    if response.success:
        token = response.token
        print(f"Login successful. Token: {token}")
        return token
    else:
        print("Login failed. Please try again.")
        return None

def post_data(stub, token):
    data_type = input("Enter the type of data (e.g., assignment, query): ")
    data = input("Enter the data to post: ")
    
    post_response = stub.Post(lms_pb2.PostRequest(token=token, type=data_type, data=data))
    print(f"Post status: {post_response.message}")

def get_data(stub, token):
    data_type = input("Enter the type of data to retrieve (e.g., assignment, query): ")
    
    get_response = stub.Get(lms_pb2.GetRequest(token=token, type=data_type))
    for item in get_response.data:
        print(f"Retrieved: {item.data}")

def ask_llm_query(stub, token):
    query = input("Enter your query for the LLM: ")
    
    query_response = stub.getLLMAnswer(lms_pb2.getLLMAnswerRequest(token=token, queryId="1", query=query))
    print(f"LLM Response: {query_response.answer}")

def logout(stub, token):
    logout_response = stub.Logout(lms_pb2.LogoutRequest(token=token))
    print(f"Logout status: {logout_response.message}")

def run():
    with grpc.insecure_channel('127.0.0.1:50051') as channel:
        stub = lms_pb2_grpc.LMSServiceStub(channel)
        
        token = None
        
        while not token:
            token = login(stub)
        
        while True:
            print("\nOptions: ")
            print("1: Post Data")
            print("2: Get Data")
            print("3: Ask LLM a Query")  # New option added
            print("4: Logout")
            
            choice = input("Choose an option: ")
            
            if choice == '1':
                post_data(stub, token)
            elif choice == '2':
                get_data(stub, token)
            elif choice == '3':
                ask_llm_query(stub, token)  # Call the function for LLM query
            elif choice == '4':
                logout(stub, token)
                break
            else:
                print("Invalid choice. Please try again.")

if __name__ == '__main__':
    run()

# Source: ChatGPT