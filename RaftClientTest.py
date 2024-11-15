import grpc
import lms_pb2
import lms_pb2_grpc
import time

class LMSClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses  # Dictionary of node addresses in the format {node_id: 'ip:port', ...}
        self.current_leader = None  # Track the current leader's address
        self.token = None  # Store the session token after login

    def create_stub(self, address):
        """Create a gRPC stub for the specified address."""
        channel = grpc.insecure_channel(address)
        stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
        return stub

    def login(self, username, password):
        """Login to the LMS server via the leader node."""
        try:
            leader_address = self.get_leader_address()
            if leader_address:
                stub = self.create_stub(leader_address)
                response = stub.Login(lms_pb2.LoginRequest(username=username, password=password))

                if response.success:
                    self.token = response.token
                    print(f"Login successful. Token: {self.token}")
                    return True
                else:
                    print("Login failed.")
                    return False
            else:
                print("Failed to retrieve the leader's address.")
                return False

        except grpc.RpcError as e:
            print(f"gRPC error during login: {e.details()}")
            return False

    def post(self, data_type, data):
        """Post data to the LMS server. This must be sent to the leader."""
        if self.token is None:
            print("Please login first.")
            return

        try:
            leader_address = self.get_leader_address()
            if leader_address:
                stub = self.create_stub(leader_address)
                response = stub.Post(lms_pb2.PostRequest(token=self.token, type=data_type, data=data))

                if response.success:
                    print(f"Data posted successfully via {leader_address}.")
                    return True
                else:
                    print(f"Post failed via {leader_address}. Message: {response.message}")
                    return False
            else:
                print("Failed to retrieve the leader's address.")
                return False

        except grpc.RpcError as e:
            print(f"gRPC error during post: {e.details()}")
            return False

    def get(self, data_type):
        """Get data from the LMS server via the leader."""
        if self.token is None:
            print("Please login first.")
            return

        try:
            leader_address = self.get_leader_address()
            if leader_address:
                stub = self.create_stub(leader_address)
                print("line 77 - clientTest")
                response = stub.Get(lms_pb2.GetRequest(token=self.token, type=data_type)) 
                print("line 79 - clientTest")
                if response.data:
                    print(f"Data retrieved from {leader_address}:")
                    for item in response.data:
                        print(f"- {item.type_id}: {item.data}")
                    return True
                else:
                    print(f"No data found on {leader_address}.")
                    return False
            else:
                print("Failed to retrieve the leader's address.")
                return False

        except grpc.RpcError as e:
            print(f"gRPC error during get: {e.details()}")
            return False

    def logout(self):
        """Logout from the LMS server via the leader."""
        if self.token is None:
            print("You are not logged in.")
            return

        try:
            leader_address = self.get_leader_address()
            if leader_address:
                stub = self.create_stub(leader_address)
                response = stub.Logout(lms_pb2.LogoutRequest(token=self.token))

                if response.success:
                    print("Logout successful.")
                    self.token = None
                    return True
                else:
                    print(f"Logout failed via {leader_address}.")
                    return False
            else:
                print("Failed to retrieve the leader's address.")
                return False

        except grpc.RpcError as e:
            print(f"gRPC error during logout: {e.details()}")
            return False
        
    def ask_llm_query(self, query):
        if self.token is None:
            print("Please login first.")
            return

        try:
            # Use the TutoringServer stub instead of LMSRaftServiceStub
            tutoring_address = '172.17.49.232:50055'  # Replace with the actual IP/port of the TutoringServer
            channel = grpc.insecure_channel(tutoring_address)
            stub = lms_pb2_grpc.TutoringServerStub(channel)  # Correct stub for getLLMAnswer
            
            # Send the request to the TutoringServer
            response = stub.getLLMAnswer(lms_pb2.getLLMAnswerRequest(token=self.token, queryId="1", query=query))
            print(f"LLM Response: {response.answer}")
        except grpc.RpcError as e:
            print(f"Error querying LLM service: {e.details()}")

        
    

    def get_leader_address(self):
        """Fetch the leader's address by querying all nodes."""
        for node_id, address in self.node_addresses.items():
            try:
                # Create a stub to communicate with this node
                stub = self.create_stub(address)

                # Send a GetLeader request to this node
                response = stub.GetLeader(lms_pb2.GetLeaderRequest())

                if response.isLeader:
                    # This node claims to be the leader
                    print(f"Node {node_id} is the leader, using address {address}.")
                    self.current_leader = address
                    return address

                elif response.leaderId in self.node_addresses:
                    # This node is a follower but knows the leader
                    leader_address = self.node_addresses[response.leaderId]
                    print(f"Node {node_id} is a follower, leader is {response.leaderId} at address {leader_address}.")
                    self.current_leader = leader_address
                    return leader_address

            except grpc.RpcError as e:
                print(f"Error connecting to node {node_id} at address {address}: {e.details() or 'Unknown error'}")

        # If no leader is found
        print("No leader found in the cluster after checking all nodes.")
        return None


# Usage Example
if __name__ == '__main__':
    node_addresses = {
        1: '172.17.49.232:50051',  # lenovo
        2: '172.17.49.232:50052',   # asus
        3: '172.17.49.232:50053',  # aditya
        4: '172.17.49.232:50054'   # N
    }

    client = LMSClient(node_addresses)

    username = input("Enter username: ")
    password = input("Enter password: ")
    client.login(username, password)

    while True:
        print("\nOptions: ")
        print("1: Post Data")
        print("2: Get Data")
        print("3: Ask LLM a Query")  # New option added
        print("4: Logout")

        choice = input("Choose an option: ")

        if choice == '1':
            data_type = input("Enter the type of data (e.g., assignment, query): ")
            data = input("Enter the data to post: ")
            client.post(data_type, data)
        elif choice == '2':
            data_type = input("Enter the type of data (e.g., assignment, query) you want to fetch: ")
            client.get(data_type)
        elif choice == '3':
            query = input("Enter your query for LLM")
            client.ask_llm_query(query)
        elif choice == '4':
            client.logout()
            break
        else:
            print("Invalid choice. Please try again.")
