import grpc
from concurrent import futures
import time
import threading
import random
import lms_pb2
import lms_pb2_grpc
import json
import os

# Define hardcoded credentials for students and instructors
users = {
    "student": "student_pass",
    "instructor": "instructor_pass"
}

# Raft node states
FOLLOWER = 0
CANDIDATE = 1
LEADER = 2

# Raft timeout constants
MIN_ELECTION_TIMEOUT = 10 # Minimum election timeout in seconds
MAX_ELECTION_TIMEOUT = 20  # Maximum election timeout in seconds
HEARTBEAT_INTERVAL = 2  # Leader heartbeat interval in seconds

class LMSRaftServiceServicer(lms_pb2_grpc.LMSRaftServiceServicer):
    def __init__(self, node_id, port):
        self.node_id = node_id
        self.state = FOLLOWER  # Start as follower
        self.current_term = 0  # Raft term
        self.voted_for = None  # Track who we voted for
        self.log = []  # Raft log
        self.commit_index = 0  # Index of the highest log entry known to be committed
        self.last_applied = 0  # Index of highest log entry applied to state machine
        self.next_index = {}  # For leader, index of next log entry to send to each follower
        self.match_index = {}  # For leader, highest log entry known to be replicated on each follower

        # Leader election timers
        self.election_timer = None
        self.leader_id = None  # Track the current leader
        self.heartbeat_timer = None

        self.votes_received = 0

        # LMS state (in-memory)
        self.sessions = {}  # Token to user mapping
        # self.data_store = self.load_data_store()  # Store assignments and other data by type
        self.data_store = {}

        self.port = port
        self.node_ports = {
            'node1': '50051',
            'node2': '50052',
            'node3': '50053',
            'node4': '50054'
        }
        
        # LLM functionalities
        self.tutoring_channel = grpc.insecure_channel('localhost:50055')
        self.tutoring_stub = lms_pb2_grpc.TutoringServerStub(self.tutoring_channel)

        # Start the election timer
        self.reset_election_timer()

    ### LMS Functionality (Login, Post, Get) ###
    
    def Login(self, request, context):
        """Handle user login with username and password."""
        if request.username in users and users[request.username] == request.password:
            token = f"token-{time.time()}"  # Generate a simple token
            self.sessions[token] = request.username
            print(f"User {request.username} has logged in with Token {token}")
            # Now replicate this token to all the followers
            success = self.replicate_session_to_followers(token, request.username)
            if success:
                return lms_pb2.LoginResponse(success=True, token=token)
            else:
                # If replication to followers fails, remove the session and return failure
                del self.sessions[token]
                return lms_pb2.LoginResponse(success=False, token="")
            
        return lms_pb2.LoginResponse(success=False, token="")

    def Logout(self, request, context):
        """Handle user logout."""
        if request.token in self.sessions:
            del self.sessions[request.token]
            
            return lms_pb2.StatusResponse(success=True, message="Logged out successfully")
        return lms_pb2.StatusResponse(success=False, message="Invalid token")

    def Post(self, request, context):
        """Handle posting data (leader only)."""
        if self.state != LEADER:
            return lms_pb2.StatusResponse(success=False, message="Only leader can handle writes.")

        if request.token in self.sessions:
            # Ensure that request.type is a valid key in data_store
            if request.type not in self.data_store:
                self.data_store[request.type] = []  # Initialize as an empty list for this type
            
            self.data_store[request.type].append(request.data)
           
            log_entry = {'term': self.current_term, 'type': request.type, 'data': request.data}
            self.log.append(log_entry)
            print(self.log)
            # After log replication, send the data_store update
            if self.replicate_data_store_to_followers(request.type, self.data_store[request.type]):
                return lms_pb2.StatusResponse(success=True, message="Post replicated and committed")
            else:
                return lms_pb2.StatusResponse(success=False, message="Log replicated but failed to replicate data store")
        return lms_pb2.StatusResponse(success=False, message="Invalid token")

    def Get(self, request, context):
        """Handle getting data."""
        if request.token in self.sessions:
            data = self.data_store.get(request.type, [])
            data_items = [lms_pb2.DataItem(type_id=str(i), data=item) for i, item in enumerate(data)]
            return lms_pb2.GetResponse(data=data_items)
        return lms_pb2.GetResponse(data=[])

    ### Raft Functionality (Leader Election, Heartbeats) ###

    def reset_election_timer(self):
        """Reset election timer and convert to candidate if timeout occurs."""
        if self.election_timer:
            self.election_timer.cancel()

        # Randomize election timeout for staggered elections
        timeout = random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
        print(f"Node {self.node_id} resetting election timer with timeout {timeout} seconds.")
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start()

    def start_election(self):
        """Convert to candidate and start a new election."""
        print(f"Node {self.node_id} is starting an election for term {self.current_term + 1}.")
        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1  # Vote for self
        active_nodes = self.get_active_nodes()

        if active_nodes == 1:
            self.state = LEADER
            self.become_leader()

        # Send RequestVote RPCs to all other nodes
        for node, port in self.node_ports.items():
            if node != self.node_id and active_nodes > 1:
                request_vote_request = lms_pb2.RequestVoteRequest(
                    candidateId=self.node_id,
                    term=self.current_term,
                    lastLogIndex=len(self.log) - 1,
                    lastLogTerm=self.log[-1]['term'] if self.log else 0
                )
                self.send_request_vote(port, request_vote_request)

        # If no majority votes are received, restart the election timer
        self.reset_election_timer()

    def send_request_vote(self, port, request_vote_request):
        """Send RequestVote RPC to another node."""
        try:
            print(f"Node {self.node_id} sending RequestVote to port {port}")
            channel = grpc.insecure_channel(f'localhost:{port}')
            stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
            response = stub.RequestVote(request_vote_request)
            print(f"Node {self.node_id} received response from port {port}")
            self.handle_vote_response(response)
        except grpc.RpcError as e:
            print(f"Error sending RequestVote to port {port}")

    def handle_vote_response(self, response):
        """Handle response to a vote request."""
        if response.term > self.current_term:
            self.current_term = response.term
            self.state = FOLLOWER
            self.voted_for = None
            print(f"Node {self.node_id} stepped down to follower. Term: {self.current_term}")
            return

        if self.state == CANDIDATE and response.voteGranted:
            self.votes_received += 1
            print(f"Node {self.node_id} received a vote. Total votes: {self.votes_received}")

            if self.votes_received > (self.get_active_nodes() // 2):
                self.become_leader()

    def become_leader(self):
        """Convert to leader and start sending heartbeats."""
        self.state = LEADER
        self.leader_id = self.node_id
        print(f"Node {self.node_id} became the leader for term {self.current_term}.")
        self.reset_heartbeat_timer()

    def reset_heartbeat_timer(self):
        """Send heartbeats to maintain leadership."""
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = threading.Timer(HEARTBEAT_INTERVAL, self.send_heartbeats)
        self.heartbeat_timer.start()

    def send_heartbeats(self):
        """Send heartbeats to all followers."""
        if self.state == LEADER:
            print(f"Node {self.node_id} sending heartbeats.")
            for node, port in self.node_ports.items():
                if node != self.node_id:
                    append_entries_request = lms_pb2.AppendEntriesRequest(
                        leaderId=self.node_id,
                        term=self.current_term,
                        prevLogIndex=len(self.log) - 1,
                        prevLogTerm=self.log[-1]['term'] if self.log else 0,
                        entries=[],  # Heartbeat does not include log entries
                        leaderCommit=self.commit_index
                    )
                    if self.get_active_nodes() > 1:
                        self.send_append_entries(node, port, append_entries_request)

            # Check if self.data_store is initialized
            if self.data_store is None:
                print("Warning: data_store was None, initializing as an empty dictionary.")
                self.data_store = {}
        
        if isinstance(self.data_store, dict):
            # Replicate data store during heartbeats

            for data_type, data_list in self.data_store.items():
                self.replicate_data_store_to_followers(data_type, data_list)
            
            for token, user in self.sessions.items():
                self.replicate_session_to_followers(token, user)

        else:
            print(f"Error: self.data_store is of type {type(self.data_store)}, expected dict.")

            self.reset_heartbeat_timer()

    def send_append_entries(self, node, port, append_entries_request):
        """Send AppendEntries RPC to a follower."""
        try:
            channel = grpc.insecure_channel(f'localhost:{port}')
            stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
            response = stub.AppendEntries(append_entries_request)
            self.handle_append_entries_response(response)
        except grpc.RpcError as e:
            print(f"Error sending AppendEntries to {port}")

    def handle_append_entries_response(self, response):
        """Handle response to an AppendEntries RPC."""
        if response.term > self.current_term:
            self.current_term = response.term
            self.state = FOLLOWER
            self.voted_for = None
            print(f"Node {self.node_id} stepped down to follower after receiving a higher term. Term: {self.current_term}")

    def RequestVote(self, request, context):
        """Handle RequestVote RPC."""
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            self.state = FOLLOWER

        if self.voted_for is None or self.voted_for == request.candidateId:
            self.voted_for = request.candidateId
            return lms_pb2.RequestVoteReply(term=self.current_term, voteGranted=True)
        return lms_pb2.RequestVoteReply(term=self.current_term, voteGranted=False)

    def AppendEntries(self, request, context):
        """Handle AppendEntries (heartbeat or log replication) RPC."""
        if request.term < self.current_term:
            return lms_pb2.AppendEntriesReply(term=self.current_term, success=False)

        self.current_term = request.term
        self.leader_id = request.leaderId
        self.reset_election_timer()  # Reset election timer since we've heard from the leader
        print(f"Node {self.node_id} received heartbeat from leader {self.leader_id}.")

        # Respond with success
        return lms_pb2.AppendEntriesReply(term=self.current_term, success=True)

    def replicate_to_followers(self, log_entry):
        """Replicate log entry to followers."""
        success_count = 0
        for node, port in self.node_ports.items():
            if node != self.node_id:
                try:
                    channel = grpc.insecure_channel(f"localhost:{port}")
                    stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
                    response = stub.AppendEntries(
                        lms_pb2.AppendEntriesRequest(
                            leaderId=self.node_id,
                            term=self.current_term,
                            prevLogIndex=self.get_last_log_index(),
                            prevLogTerm=self.get_last_log_term(),
                            entries=[log_entry],
                            leaderCommit=self.commit_index
                        )
                    )
                    if response.success:
                        success_count += 1
                except Exception as e:
                    print(f"Error replicating to follower {node}")

        return success_count >= (self.get_active_nodes() // 2)

    def get_last_log_index(self):
        return len(self.log) - 1 if self.log else 0

    def get_last_log_term(self):
        return self.log[-1]['term'] if self.log else self.current_term

    def GetLeader(self, request, context):
        """Respond with whether this node is the leader, and the leader's ID."""
        if self.state == LEADER:
            return lms_pb2.GetLeaderReply(isLeader=True, leaderId=self.node_id)
        else:
            return lms_pb2.GetLeaderReply(isLeader=False, leaderId=self.leader_id if self.leader_id else "")

    def get_last_log_index(self):
        if len(self.log) == 0:
            return 0
        return len(self.log) - 1
    
    def get_last_log_term(self):
        if len(self.log) == 0:
            return self.current_term
        return self.log[-1]['term']
    
    def replicate_session_to_followers(self, token, username):
        """Replicate session (token, username) to all followers."""
        success_count = 0
        follower_addresses = []
    
        for node, port in self.node_ports.items():
            if node != self.node_id:  # Skip the leader itself
                follower_addresses.append(f"localhost:{port}")
    
        for follower in follower_addresses:
            try:
                # Create a gRPC stub for the follower
                channel = grpc.insecure_channel(follower)
                stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
            
                # Send a request to the follower to add the session
                response = stub.AddSession(lms_pb2.AddSessionRequest(token=token, username=username))
            
                if response.success:
                    success_count += 1
            except grpc.RpcError as e:
                print(f"Failed to replicate session to follower {follower}")
    
        # Return True if a majority of followers successfully replicated the session
        if(self.get_majority_count() == 1):
            return True
        return success_count >= self.get_majority_count()


    def replicate_data_store_to_followers(self, data_type, data_list):
        """Replicate the data_store to all followers after log replication."""
        success_count = 0
        follower_addresses = []
    
        for node, port in self.node_ports.items():
            if node != self.node_id:
                follower_addresses.append("localhost:" + port)
    
        # Iterate over all the followers
            for follower in follower_addresses:
                try:
                    # Create a gRPC stub for the follower
                    channel = grpc.insecure_channel(follower)
                    stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
            
                    # Send the ReplicateDataStore RPC to synchronize the data_store
                    response = stub.ReplicateDataStore(lms_pb2.ReplicateDataStoreRequest(
                        leaderId=self.leader_id,
                        type=data_type,
                        data=data_list  # Send the full list of data for the given type
                    ), timeout = 1)
            
                    if response.success:
                        success_count += 1
                        # print(f"{success_count} time success")
                    else:
                        print(f"Data replication failed to {follower}")

                except grpc.RpcError as e:
                    print(f"Failed to replicate data store to follower {follower}")
    
        # Check if the majority of followers replicated the data_store
        if(self.get_majority_count() == 1):
            return True
        return success_count >= self.get_majority_count()

    
    def AddSession(self, request, context):
        """Add session (token and username) received from the leader."""
        self.sessions[request.token] = request.username
        print(f"User {request.username} with Token {request.token} is currently active with leader {self.leader_id}.")
        return lms_pb2.StatusResponse(success=True)
    
    def get_majority_count(self):
        # total_nodes = len(self.node_ports)
        total_nodes = self.get_active_nodes()
        return (total_nodes // 2) + 1
    

    def get_active_nodes(self):
        active_nodes = 0
        
        for node, port in self.node_ports.items():
            try:
                # Try to establish a connection to the node
                channel = grpc.insecure_channel(f"localhost:{port}")
                stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
                
                # You can use any lightweight RPC method to test if the node is alive, like GetLeader
                response = stub.GetLeader(lms_pb2.GetLeaderRequest())
                
                # If we receive a response, consider the node active
                if response:
                    active_nodes += 1
            except grpc.RpcError as e:
                # Node is not available, log the error and continue
                print(f"Node {node} is down or not responding")
        print(f"Number of Current Active nodes: {active_nodes}")
        return active_nodes
    
    def ReplicateDataStore(self, request, context):
        if request.type not in self.data_store:
            self.data_store[request.type] = []
        self.data_store[request.type] = list(request.data)
        print(f"Follower {self.node_id} updated data store: {self.data_store}")
        return lms_pb2.StatusResponse(success=True, message="Data store replicated successfully")
    
    def getLLMAnswer(self, request, context):
        # Forward query to the tutoring server
        tutoring_response = self.tutoring_stub.getLLMAnswer(
            lms_pb2.getLLMAnswerRequest(queryId=request.queryId, query=request.query))
        return tutoring_response
    
    def save_data_store(self):
        """Save the current data_store to a file."""
        file_name = f"data_store_{self.node_id}.json"
        try:
            with open(file_name, 'w') as file:
                json.dump(self.data_store, file)
            print(f"Data store saved successfully for node {self.node_id}.")
        except Exception as e:
            print(f"Error saving data store for node {self.node_id}: {e}")
    
    def load_data_store(self):
        """Load the data_store from a file, if it exists."""
        file_name = f"data_store_{self.node_id}.json"
        if os.path.exists(file_name):
            try:
                with open(file_name, 'r') as file:
                    data = json.load(file)
                    if (isinstance(data,dict)):
                        self.data_store = data
                    else:
                        print("Warning: data_store.json did not contain a dictionary. Initializing as empty dictionary.")
                        self.data_store = {}
                print(f"Data store loaded successfully for node {self.node_id}.")
            except Exception as e:
                print(f"Error loading data store for node {self.node_id}: {e}")
                self.data_store = {}  # Initialize an empty data store on error
        else:
            print(f"No previous data store found for node {self.node_id}, starting with an empty data store.")
            self.data_store = {}
    
    def get_leader_address(self, max_retries=10, retry_delay=2):
        """Retrieve the address of the current leader node, with retry logic."""
        for attempt in range(max_retries):
            try:
                if self.leader_id is None:
                    raise Exception("Leader is not known yet.")

                if self.leader_id in self.node_ports:
                    leader_port = self.node_ports[self.leader_id]
                    leader_address = f"localhost:{leader_port}"
                    return leader_address
                else:
                    raise Exception(f"Leader ID {self.leader_id} is not recognized.")
        
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Optional: exponential backoff
                else:
                    print(f"All {max_retries} attempts failed. Error: {e}")
                    raise  # Re-raise the exception after all retries are exhausted


def serve(node_id, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_servicer = LMSRaftServiceServicer(node_id, port)

    lms_pb2_grpc.add_LMSRaftServiceServicer_to_server(raft_servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    print(f"LMS Raft-based Server {node_id} started on port {port}.")
    
    # After starting the server, check if the node is a follower
    if raft_servicer.state == FOLLOWER:

        print(f"Node {node_id} is a follower. Fetching data_store from local files.")
        raft_servicer.load_data_store()
        # if not raft_servicer.data_store:
        #     print(f"Node {node_id} is a follower. Fetching data_store from leader.")
        #     raft_servicer.fetch_data_store_from_leader()  # Fetch the latest data_store from the leader
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print(f"Shutting down the server {node_id}.")
        raft_servicer.save_data_store()  # Save the data store before shutting down
        server.stop(0)

if __name__ == '__main__':
    import sys

    if len(sys.argv) != 3:
        print("Usage: python raft_server.py <node_id> <port>")
        sys.exit(1)

    node_id = sys.argv[1]
    port = sys.argv[2]

    serve(node_id, port)

