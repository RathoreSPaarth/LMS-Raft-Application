import grpc
import lms_pb2
import lms_pb2_grpc
import time

class LMSClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses  # List of node addresses in the format ['localhost:50051', ...]
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
            # Get the leader port first
            leader_port = self.get_leader_port()

            if leader_port:
                # Create a stub using the leader's port
                leader_address = f'localhost:{leader_port}'
                stub = self.create_stub(leader_address)
                # Attempt to login via the leader
                response = stub.Login(lms_pb2.LoginRequest(username=username, password=password)) 

                if response.success:
                    self.token = response.token
                    print(f"Login successful. Token: {self.token}")
                    return True
                else:
                    print("Login failed.")
                    return False
            else:
                print("Failed to retrieve the leader's port.")
                return False

        except grpc.RpcError as e:
            print(f"Failed to login via the leader node")
            return False

    def post(self, data_type, data):
        """Post data to the LMS server. This must be sent to the leader."""
        if self.token is None:
            print("Please login first.")
            return

        try:
            # Ensure the leader is known
            leader_port = self.get_leader_port()
            leader_address = f'localhost:{leader_port}'
            print(leader_address)
            # Send post request to the leader node
            stub = self.create_stub(leader_address)
            response = stub.Post(lms_pb2.PostRequest(token=self.token, type=data_type, data=data))

            if response.success:
                print(f"Data posted successfully via {leader_address}.")
                return True
            else:
                print(f"Post failed via {leader_address}. Message: {response.message}")
                return False

        except grpc.RpcError as e:
            print(f"Error posting data via leader node")
            return False

    def get(self, data_type):
        """Get data from the LMS server via the leader."""
        if self.token is None:
            print("Please login first.")
            return

        try:
            # Ensure the leader is known
            leader_port = self.get_leader_port()
            leader_address = f'localhost:{leader_port}'

            # Send get request to the leader node
            stub = self.create_stub(leader_address)
            response = stub.Get(lms_pb2.GetRequest(token=self.token, type=data_type))
            if response.data:
                print(f"Data retrieved from {leader_address}:")
                for item in response.data:
                    print(f"- {item.type_id}: {item.data}")
                return True
            else:
                print(f"No data found on {leader_address}.")
                return False

        except grpc.RpcError as e:
            print(f"Error retrieving data via leader node")
            return False

    def logout(self):
        """Logout from the LMS server via the leader."""
        if self.token is None:
            print("You are not logged in.")
            return

        try:
            leader_port = self.get_leader_port()
            leader_address = f'localhost:{leader_port}'

            # Send logout request to the leader node
            stub = self.create_stub(leader_address)
            response = stub.Logout(lms_pb2.LogoutRequest(token=self.token))

            if response.success:
                print("Logout successful.")
                self.token = None
                return True
            else:
                print(f"Logout failed via {leader_address}.")
                return False

        except grpc.RpcError as e:
            print(f"Failed to logout via leader node")
            return False

    def get_leader_port(self):
        """Fetch the leader's port by querying the nodes."""
        node_ports = {
            'node1': '50051',
            'node2': '50052',
            'node3': '50053',
            'node4': '50054'
        }

        for node, port in node_ports.items():
            try:
                # Connect to the node and request the leader status
                channel = grpc.insecure_channel(f'localhost:{port}')
                stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
                response = stub.GetLeader(lms_pb2.GetLeaderRequest())

                # If this node is the leader, return its port
                if response.isLeader:
                    print(f"Node {node} is the leader, using port {port}.")
                    self.current_leader = f"localhost:{port}"
                    return port

                # If this node knows the leader, return the leader's port
                elif response.leaderId:
                    leader_port = node_ports[response.leaderId]
                    print(f"Node {node} is a follower, leader is {response.leaderId} using port {leader_port}.")
                    self.current_leader = f"localhost:{leader_port}"
                    return leader_port

            except Exception as e:
                print(f"Error connecting to node {node} at port {port}")

        # If no leader is found, raise an exception
        raise Exception("No leader found in the cluster.")

# Usage Example
if __name__ == '__main__':
    node_addresses = ['localhost:50051', 'localhost:50052', 'localhost:50053', 'localhost:50054']  # Update with your node addresses

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
        elif choice == '4':
            client.logout()
            break
        else:
            print("Invalid choice. Please try again.")
