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
MIN_ELECTION_TIMEOUT = 10  # Minimum election timeout in seconds
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
        self.data_store = {}

        self.port = port
        # Define IP addresses and ports for each node
        self.node_ports = {
            'node1': '172.17.49.87:50051',  # ASUS
            'node2': '172.17.49.125:50052', # Aditya
            'node3': '172.17.49.183:50053', # Nishit
            'node4': '172.17.49.190:50054'  # Additional Node
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
            if request.type not in self.data_store:
                self.data_store[request.type] = []  # Initialize as an empty list for this type
            
            self.data_store[request.type].append(request.data)
           
            log_entry = {'term': self.current_term, 'type': request.type, 'data': request.data}
            self.log.append(log_entry)
            print(self.log)
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
        if self.election_timer:
            self.election_timer.cancel()

        # Randomize election timeout for staggered elections
        timeout = random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
        print(f"Node {self.node_id} resetting election timer with timeout {timeout} seconds.")
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start()

    def start_election(self):
        print(f"Node {self.node_id} is starting an election for term {self.current_term + 1}.")
        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1  # Vote for self
        active_nodes = self.get_active_nodes()

        if active_nodes == 1:
            self.state = LEADER
            self.become_leader()

        for node, address in self.node_ports.items():
            if node != self.node_id and active_nodes > 1:
                request_vote_request = lms_pb2.RequestVoteRequest(
                    candidateId=self.node_id,
                    term=self.current_term,
                    lastLogIndex=len(self.log) - 1,
                    lastLogTerm=self.log[-1]['term'] if self.log else 0
                )
                self.send_request_vote(address, request_vote_request)

        self.reset_election_timer()

    def send_request_vote(self, address, request_vote_request):
        try:
            print(f"Node {self.node_id} sending RequestVote to {address}")
            channel = grpc.insecure_channel(address)
            stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
            response = stub.RequestVote(request_vote_request)
            print(f"Node {self.node_id} received response from {address}")
            self.handle_vote_response(response)
        except grpc.RpcError as e:
            print(f"Error sending RequestVote to {address}")

    def handle_vote_response(self, response):
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
        self.state = LEADER
        self.leader_id = self.node_id
        print(f"Node {self.node_id} became the leader for term {self.current_term}.")
        self.reset_heartbeat_timer()

    def reset_heartbeat_timer(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = threading.Timer(HEARTBEAT_INTERVAL, self.send_heartbeats)
        self.heartbeat_timer.start()

    def send_heartbeats(self):
        if self.state == LEADER:
            print(f"Node {self.node_id} sending heartbeats.")
            for node, address in self.node_ports.items():
                if node != self.node_id:
                    append_entries_request = lms_pb2.AppendEntriesRequest(
                        leaderId=self.node_id,
                        term=self.current_term,
                        prevLogIndex=len(self.log) - 1,
                        prevLogTerm=self.log[-1]['term'] if self.log else 0,
                        entries=[],  # Heartbeat does not include log entries
                        leaderCommit=self.commit_index
                    )
                    self.send_append_entries(address, append_entries_request)
            self.reset_heartbeat_timer()

    def send_append_entries(self, address, append_entries_request):
        try:
            channel = grpc.insecure_channel(address)
            stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
            response = stub.AppendEntries(append_entries_request)
            self.handle_append_entries_response(response)
        except grpc.RpcError as e:
            print(f"Error sending AppendEntries to {address}")

    def handle_append_entries_response(self, response):
        if response.term > self.current_term:
            self.current_term = response.term
            self.state = FOLLOWER
            self.voted_for = None
            print(f"Node {self.node_id} stepped down to follower after receiving a higher term. Term: {self.current_term}")

    def replicate_session_to_followers(self, token, username):
        success_count = 0
        for node, address in self.node_ports.items():
            if node != self.node_id:  # Skip the leader itself
                try:
                    channel = grpc.insecure_channel(address)
                    stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
                    response = stub.AddSession(lms_pb2.AddSessionRequest(token=token, username=username))
                    if response.success:
                        success_count += 1
                except grpc.RpcError as e:
                    print(f"Failed to replicate session to follower {address}")
        return success_count >= self.get_majority_count()

    def replicate_data_store_to_followers(self, data_type, data_list):
        success_count = 0
        for node, address in self.node_ports.items():
            if node != self.node_id:
                try:
                    channel = grpc.insecure_channel(address)
                    stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
                    response = stub.ReplicateDataStore(lms_pb2.ReplicateDataStoreRequest(
                        leaderId=self.leader_id,
                        type=data_type,
                        data=data_list
                    ))
                    if response.success:
                        success_count += 1
                except grpc.RpcError as e:
                    print(f"Failed to replicate data store to follower {address}")
        return success_count >= self.get_majority_count()

    def AddSession(self, request, context):
        self.sessions[request.token] = request.username
        print(f"User {request.username} with Token {request.token} is currently active with leader {self.leader_id}.")
        return lms_pb2.StatusResponse(success=True)
    
    def get_majority_count(self):
        total_nodes = self.get_active_nodes()
        return (total_nodes // 2) + 1
    
    def get_active_nodes(self):
        active_nodes = 0
        for node, address in self.node_ports.items():
            try:
                channel = grpc.insecure_channel(address)
                stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
                response = stub.GetLeader(lms_pb2.GetLeaderRequest())
                if response:
                    active_nodes += 1
            except grpc.RpcError:
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
        tutoring_response = self.tutoring_stub.getLLMAnswer(
            lms_pb2.getLLMAnswerRequest(queryId=request.queryId, query=request.query))
        return tutoring_response
    
    def save_data_store(self):
        file_name = f"data_store_{self.node_id}.json"
        try:
            with open(file_name, 'w') as file:
                json.dump(self.data_store, file)
            print(f"Data store saved successfully for node {self.node_id}.")
        except Exception as e:
            print(f"Error saving data store for node {self.node_id}: {e}")
    
    def load_data_store(self):
        file_name = f"data_store_{self.node_id}.json"
        if os.path.exists(file_name):
            try:
                with open(file_name, 'r') as file:
                    data = json.load(file)
                    if isinstance(data, dict):
                        self.data_store = data
                    else:
                        print("Warning: data_store.json did not contain a dictionary. Initializing as empty dictionary.")
                        self.data_store = {}
                print(f"Data store loaded successfully for node {self.node_id}.")
            except Exception as e:
                print(f"Error loading data store for node {self.node_id}: {e}")
                self.data_store = {}
        else:
            print(f"No previous data store found for node {self.node_id}, starting with an empty data store.")
            self.data_store = {}

    def get_leader_address(self, max_retries=10, retry_delay=2):
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
                    retry_delay *= 2
                else:
                    print(f"All {max_retries} attempts failed. Error: {e}")
                    raise

def serve(node_id, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_servicer = LMSRaftServiceServicer(node_id, port)
    lms_pb2_grpc.add_LMSRaftServiceServicer_to_server(raft_servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    
    server.start()
    
    print(f"LMS Raft-based Server {node_id} started on port {port}.")
    
    if raft_servicer.state == FOLLOWER:
        print(f"Node {node_id} is a follower. Fetching data_store from local files.")
        raft_servicer.load_data_store()
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print(f"Shutting down the server {node_id}.")
        raft_servicer.save_data_store()
        server.stop(0)

if __name__ == '__main__':
    import sys

    if len(sys.argv) != 3:
        print("Usage: python raft_server.py <node_id> <port>")
        sys.exit(1)

    node_id = sys.argv[1]
    port = sys.argv[2]

    serve(node_id, port)
