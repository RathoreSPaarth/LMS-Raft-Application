import grpc
from concurrent import futures
import time
import threading
import random
import lms_pb2
import lms_pb2_grpc
import json
import os
import logging  # Import logging module

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s [Node %(node_id)s] %(message)s')

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
MIN_ELECTION_TIMEOUT = 3  # Minimum election timeout in seconds
MAX_ELECTION_TIMEOUT = 6  # Maximum election timeout in seconds
HEARTBEAT_INTERVAL = 1  # Leader heartbeat interval in seconds

class LMSRaftServiceServicer(lms_pb2_grpc.LMSRaftServiceServicer):
    def __init__(self, node_id, port):
        self.node_id = node_id
        self.state = FOLLOWER  # Start as follower
        self.current_term = 0  # Raft term
        self.voted_for = None  # Track who we voted for
        self.log = []          # Raft log
        self.commit_index = 0  # Index of the highest log entry known to be committed
        self.last_applied = 0  # Index of highest log entry applied to state machine
        self.next_index = {}   # For leader, index of next log entry to send to each follower
        self.match_index = {}  # For leader, highest log entry known to be replicated on each follower

        # Leader election timers
        self.election_timer = None
        self.leader_id = -1  # Track the current leader
        self.heartbeat_timer = None

        self.votes_received = 0

        # LMS state (in-memory)
        self.sessions = {}     # Token to user mapping
        self.data_store = {}   # Store assignments and other data by type

        self.port = port

        self.node_addresses = {
            1: {"ip": "172.17.49.232", "port": 50051},  # lenovo
            2: {"ip": "172.17.49.232", "port": 50052},   # asus
            3: {"ip": "172.17.49.232", "port": 50053},  # aditya
            4: {"ip": "172.17.49.232", "port": 50054}   # N
        }

        # LLM functionalities
        self.tutoring_channel = grpc.insecure_channel('172.17.49.232:50055')
        self.tutoring_stub = lms_pb2_grpc.TutoringServerStub(self.tutoring_channel)

        # Start the election timer
        self.reset_election_timer()

    ### LMS Functionality (Login, Post, Get) ###

    def Login(self, request, context):
        """Handle user login with username and password."""
        if request.username in users and users[request.username] == request.password:
            token = f"token-{time.time()}"  # Generate a simple token
            self.sessions[token] = request.username
            logging.info(f"User {request.username} has logged in with Token {token}", extra={'node_id': self.node_id})
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
            logging.info(f"User with Token {request.token} has logged out.", extra={'node_id': self.node_id})
            return lms_pb2.StatusResponse(success=True, message="Logged out successfully")
        return lms_pb2.StatusResponse(success=False, message="Invalid token")

    def Post(self, request, context):
        """Handle posting data (leader only)."""
        if self.state != LEADER:
            logging.warning("Only leader can handle writes.", extra={'node_id': self.node_id})
            return lms_pb2.StatusResponse(success=False, message="Only leader can handle writes.")

        if request.token in self.sessions:
            # Ensure that request.type is a valid key in data_store
            if request.type not in self.data_store:
                self.data_store[request.type] = []  # Initialize as an empty list for this type

            self.data_store[request.type].append(request.data)

            log_entry = {'term': self.current_term, 'type': request.type, 'data': request.data}
            self.log.append(log_entry)
            # logging.info(f"Appended to log: {log_entry}", extra={'node_id': self.node_id})
            # After log replication, send the data_store update
            if self.replicate_data_store_to_followers(request.type, self.data_store[request.type]):
                return lms_pb2.StatusResponse(success=True, message="Post replicated and committed")
            else:
                return lms_pb2.StatusResponse(success=False, message="Log replicated but failed to replicate data store")
        return lms_pb2.StatusResponse(success=False, message="Invalid token")

    # def Get(self, request, context):
    #     """Handle getting data with detailed error handling and logging."""
    #     print("inside get function on server side")
        
    #     if request.token in self.sessions:
    #         print("inside get function - if statement - on server side")
    #         data = self.data_store.get(request.type, [])
    #         print("Fetched data:", data)  # Log the fetched data
            
    #         data_items = []
    #         for i, item in enumerate(data):
    #             try:
    #                 # Ensure item is a string; convert if necessary
    #                 if not isinstance(item, str):
    #                     item = str(item)
    #                 print(f"Creating DataItem for item {i}: {item}")
                    
    #                 # Attempt to create DataItem
    #                 data_item = lms_pb2.DataItem(type_id=str(i), data=item)
    #                 # data_item = lms_pb2.DataItem(type_id="0", data="sample test data please help!")
    #                 print(f"Successfully created DataItem for item {i}. Below list data_item")
    #                 print(data_item)
    #                 data_items.append(data_item)
    #             except Exception as e:
    #                 print(f"Error creating DataItem for item {i}")
    #                 logging.error(f"Error creating DataItem for item {i}", extra={'node_id': self.node_id})
            
    #         # If successfully created data items, return them
    #         print("this is the data_items - ")
    #         print(data_items)
    #         if data_items:
    #             print("inside get function - fetched data_items - on server side")
    #             logging.debug(f"Get request for type '{request.type}': {data_items}", extra={'node_id': self.node_id})
    #             return lms_pb2.GetResponse(data=data_items)
    #         else:
    #             print("No valid data items to return.")
    #             return lms_pb2.GetResponse(data=[])
        
    #     # If token is invalid, log and return empty response
    #     logging.warning("Invalid token for Get request.", extra={'node_id': self.node_id})
    #     return lms_pb2.GetResponse(data=[])

    def Get(self, request, context):
        try:
            logging.debug("inside get function on server side", extra={'node_id': self.node_id})
            
            if request.token in self.sessions:
                logging.debug("Valid token received", extra={'node_id': self.node_id})
                data = self.data_store.get(request.type, [])
                logging.debug(f"Fetched data: {data}", extra={'node_id': self.node_id})
                
                data_items = []
                for i, item in enumerate(data):
                    try:
                        # Ensure item is a string; convert if necessary
                        if not isinstance(item, str):
                            item = str(item)
                        logging.debug(f"Creating DataItem for item {i}: {item}", extra={'node_id': self.node_id})
                        
                        # Attempt to create DataItem
                        data_item = lms_pb2.DataItem(type_id=str(i), content=item)
                        data_items.append(data_item)
                    except Exception as e:
                        logging.error(f"Error creating DataItem for item {i}: {e}", extra={'node_id': self.node_id})
                
                # If successfully created data items, return them
                logging.debug(f"DataItems created: {data_items}", extra={'node_id': self.node_id})
                return lms_pb2.GetResponse(data=data_items)
            else:
                logging.warning("Invalid token for Get request.", extra={'node_id': self.node_id})
                return lms_pb2.GetResponse(data=[])
        except Exception as e:
            logging.exception(f"Exception in Get method: {e}", extra={'node_id': self.node_id})
            return lms_pb2.GetResponse(data=[])




    # def Get(self, request, context):
    #     print("inside get function on server side")
    #     try:
    #         # Check for token presence in the request and validate it
    #         if not hasattr(request, 'token') or not request.token:
    #             logging.warning("Missing or invalid token in Get request.", extra={'node_id': self.node_id})
    #             return lms_pb2.GetResponse(data=[])

    #         # Verify if the provided token is in active sessions
    #         if request.token in self.sessions:
    #             # Confirm request contains 'type' attribute
    #             if not hasattr(request, 'type') or request.type is None:
    #                 logging.warning("Get request missing 'type' field.", extra={'node_id': self.node_id})
    #                 return lms_pb2.GetResponse(data=[])

    #             # Retrieve data and validate it's a list or convert if possible
    #             data_type = request.type
    #             data = self.data_store.get(data_type, [])
    #             if not isinstance(data, list):
    #                 logging.error(f"Data for type '{data_type}' is not a list. Data: {data}", extra={'node_id': self.node_id})
    #                 return lms_pb2.GetResponse(data=[])

    #             # Create DataItem messages from data
    #             data_items = [lms_pb2.DataItem(type_id=str(i), data=item) for i, item in enumerate(data)]
    #             logging.debug(f"Get request processed for type '{data_type}', data items: {data_items}", extra={'node_id': self.node_id})
    #             return lms_pb2.GetResponse(data=data_items)
            
    #         # Token is invalid if not found in sessions
    #         logging.warning("Invalid token provided in Get request.", extra={'node_id': self.node_id})
    #         return lms_pb2.GetResponse(data=[])
        
    #     except Exception as e:
    #         # Log unexpected errors with traceback
    #         logging.exception("Unexpected error in Get method", extra={'node_id': self.node_id})
    #         return lms_pb2.GetResponse(data=[])



    ### Raft Functionality (Leader Election, Heartbeats) ###

    def reset_election_timer(self):
        """Reset election timer and convert to candidate if timeout occurs."""
        if self.election_timer:
            self.election_timer.cancel()

        # Randomize election timeout for staggered elections
        timeout = random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
        logging.debug(f"Resetting election timer with timeout {timeout} seconds.", extra={'node_id': self.node_id})
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start()

    def start_election(self):
        """Convert to candidate and start a new election."""
        logging.info(f"Starting an election for term {self.current_term + 1}.", extra={'node_id': self.node_id})
        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1  # Vote for self
        # active_nodes = self.get_active_nodes()

        # If there's only one active node, declare self as the leader
        # if active_nodes == 1:
        #     self.state = LEADER
        #     self.become_leader()
        #     return

        # Send RequestVote RPCs to all other nodes
        for node_id, address in self.node_addresses.items():
            if node_id != self.node_id:
                ip = address['ip']
                port = address['port']
                request_vote_request = lms_pb2.RequestVoteRequest(
                    candidateId=self.node_id,
                    term=self.current_term,
                    lastLogIndex=len(self.log) - 1,
                    lastLogTerm=self.log[-1]['term'] if self.log else 0
                )
                # Call the send_request_vote with both IP and port
                self.send_request_vote(ip, port, request_vote_request)

        # If no majority votes are received, restart the election timer
        self.reset_election_timer()

    # def send_request_vote(self, ip, port, request_vote_request):
    #     """Send RequestVote RPC to another node."""
    #     try:
    #         logging.debug(f"Sending RequestVote to {ip}:{port}", extra={'node_id': self.node_id})
    #         # Use IP address and port of the target node
    #         channel = grpc.insecure_channel(f'{ip}:{port}')
    #         stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
    #         response = stub.RequestVote(request_vote_request)
    #         logging.debug(f"Received response from {ip}:{port}", extra={'node_id': self.node_id})
    #         self.handle_vote_response(response)
    #     except grpc.RpcError as e:
    #         logging.error(f"Error sending RequestVote to {ip}:{port}", extra={'node_id': self.node_id})

    def send_request_vote(self, ip, port, request_vote_request):
        """Send RequestVote RPC to another node."""
        def rpc_call():
            try:
                logging.debug(f"Sending RequestVote to {ip}:{port}", extra={'node_id': self.node_id})
                with grpc.insecure_channel(f'{ip}:{port}') as channel:
                    stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
                    response = stub.RequestVote(request_vote_request, timeout=5)
                    logging.debug(f"Received response from {ip}:{port}", extra={'node_id': self.node_id})
                    self.handle_vote_response(response)
            except grpc.RpcError as e:
                logging.error(f"Error sending RequestVote to {ip}:{port}: {e.details()}", extra={'node_id': self.node_id})

        threading.Thread(target=rpc_call).start()


    def handle_vote_response(self, response):
        """Handle response to a vote request."""
        if response.term > self.current_term:
            self.current_term = response.term
            self.state = FOLLOWER
            self.voted_for = None
            logging.info(f"Stepped down to follower. Term: {self.current_term}", extra={'node_id': self.node_id})
            return

        if self.state == CANDIDATE and response.voteGranted:
            self.votes_received += 1
            logging.info(f"Received a vote. Total votes: {self.votes_received}", extra={'node_id': self.node_id})

            if self.votes_received >= self.get_majority_count():
                self.become_leader()

    def become_leader(self):
        """Convert to leader and start sending heartbeats."""
        self.state = LEADER
        self.leader_id = self.node_id
        logging.info(f"Became the leader for term {self.current_term}.", extra={'node_id': self.node_id})
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
            logging.debug("Sending heartbeats.", extra={'node_id': self.node_id})
            for node_id, address in self.node_addresses.items():
                if node_id != self.node_id:
                    ip = address['ip']
                    port = address['port']
                    append_entries_request = lms_pb2.AppendEntriesRequest(
                        leaderId=self.node_id,
                        term=self.current_term,
                        prevLogIndex=len(self.log) - 1,
                        prevLogTerm=self.log[-1]['term'] if self.log else 0,
                        entries=[],  # Heartbeat does not include log entries
                        leaderCommit=self.commit_index
                    )
                    self.send_append_entries(ip, port, append_entries_request)

            # Check if self.data_store is initialized
            if self.data_store is None:
                logging.warning("data_store was None, initializing as an empty dictionary.", extra={'node_id': self.node_id})
                self.data_store = {}

            # Replicate data store and sessions during heartbeats
            if isinstance(self.data_store, dict):
                for data_type, data_list in self.data_store.items():
                    self.replicate_data_store_to_followers(data_type, data_list)

                for token, user in self.sessions.items():
                    self.replicate_session_to_followers(token, user)
            else:
                logging.error(f"data_store is of type {type(self.data_store)}, expected dict.", extra={'node_id': self.node_id})

        # Reset heartbeat timer
        self.reset_heartbeat_timer()

    # def send_append_entries(self, ip, port, append_entries_request):
    #     """Send AppendEntries RPC to a follower."""
    #     try:
    #         logging.debug(f"Sending AppendEntries to {ip}:{port}", extra={'node_id': self.node_id})
    #         # Use the IP address and port of the target follower node
    #         channel = grpc.insecure_channel(f'{ip}:{port}')
    #         stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
    #         response = stub.AppendEntries(append_entries_request)
    #         self.handle_append_entries_response(response)
    #     except grpc.RpcError as e:
    #         logging.error(f"Error sending AppendEntries to {ip}:{port}", extra={'node_id': self.node_id})

    def send_append_entries(self, ip, port, append_entries_request):
        """Send AppendEntries RPC to a follower."""
        def rpc_call():
            try:
                # logging.debug(f"Sending AppendEntries to {ip}:{port}", extra={'node_id': self.node_id})
                with grpc.insecure_channel(f'{ip}:{port}') as channel:
                    stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
                    response = stub.AppendEntries(append_entries_request, timeout=5)
                    self.handle_append_entries_response(response)
            except grpc.RpcError as e:
                logging.error(f"Error sending AppendEntries to {ip}:{port}: {e.details()}", extra={'node_id': self.node_id})

        threading.Thread(target=rpc_call).start()


    def handle_append_entries_response(self, response):
        """Handle response to an AppendEntries RPC."""
        if response.term > self.current_term:
            self.current_term = response.term
            self.state = FOLLOWER
            self.voted_for = None
            logging.info(f"Stepped down to follower after receiving a higher term. Term: {self.current_term}", extra={'node_id': self.node_id})

    def RequestVote(self, request, context):
        """Handle RequestVote RPC."""
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            self.state = FOLLOWER

        if self.voted_for is None or self.voted_for == request.candidateId:
            self.voted_for = request.candidateId
            logging.debug(f"Voted for candidate {request.candidateId} in term {self.current_term}.", extra={'node_id': self.node_id})
            return lms_pb2.RequestVoteReply(term=self.current_term, voteGranted=True)
        # logging.debug(f"Did not vote for candidate {request.candidateId} in term {self.current_term}.", extra={'node_id': self.node_id})
        return lms_pb2.RequestVoteReply(term=self.current_term, voteGranted=False)

    def AppendEntries(self, request, context):
        """Handle AppendEntries (heartbeat or log replication) RPC."""
        if request.term < self.current_term:
            return lms_pb2.AppendEntriesReply(term=self.current_term, success=False)

        self.current_term = request.term
        self.leader_id = request.leaderId
        self.reset_election_timer()  # Reset election timer since we've heard from the leader
        logging.debug(f"Received heartbeat from leader {self.leader_id}.", extra={'node_id': self.node_id})

        # Respond with success
        return lms_pb2.AppendEntriesReply(term=self.current_term, success=True)

    def replicate_to_followers(self, log_entry): # not called anywhere
        """Replicate log entry to followers."""
        success_count = 0
        for node_id, address in self.node_addresses.items():
            if node_id != self.node_id:
                ip = address['ip']
                port = address['port']
                try:
                    # logging.debug(f"Replicating to follower {node_id} at {ip}:{port}", extra={'node_id': self.node_id})
                    channel = grpc.insecure_channel(f"{ip}:{port}")
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
                    logging.error(f"Error replicating to follower {node_id} at {ip}:{port}", extra={'node_id': self.node_id})

        # Return True if replication succeeded on a majority of nodes
        return success_count >= self.get_majority_count()

    def get_last_log_index(self):
        return len(self.log) - 1 if self.log else 0

    def get_last_log_term(self):
        return self.log[-1]['term'] if self.log else self.current_term

    def GetLeader(self, request, context):
        """Respond with whether this node is the leader, and the leader's ID."""
        if self.state == LEADER:
            return lms_pb2.GetLeaderReply(isLeader=True, leaderId=self.node_id)
        else:
            return lms_pb2.GetLeaderReply(isLeader=False, leaderId=self.leader_id if self.leader_id else -1)

    def get_last_log_index(self):
        if len(self.log) == 0:
            return 0
        return len(self.log) - 1

    def get_last_log_term(self):
        if len(self.log) == 0:
            return self.current_term
        return self.log[-1]['term']

    # def replicate_session_to_followers(self, token, username):
    #     """Replicate session (token, username) to all followers."""
    #     success_count = 0
    #     follower_addresses = []

    #     for node_id, address in self.node_addresses.items():
    #         if node_id != self.node_id:  # Skip the leader itself
    #             ip = address['ip']
    #             port = address['port']
    #             follower_addresses.append(f"{ip}:{port}")

    #     for follower in follower_addresses:
    #         try:
    #             # Create a gRPC stub for the follower
    #             channel = grpc.insecure_channel(follower)
    #             stub = lms_pb2_grpc.LMSRaftServiceStub(channel)

    #             # Send a request to the follower to add the session
    #             response = stub.AddSession(lms_pb2.AddSessionRequest(token=token, username=username, timeout = 1))

    #             if response.success:
    #                 success_count += 1
    #         except grpc.RpcError as e:
    #             logging.error(f"Failed to replicate session to follower {follower}", extra={'node_id': self.node_id})

    #     # Return True if a majority of followers successfully replicated the session
    #     return success_count >= self.get_majority_count()

    def replicate_session_to_followers(self, token, username):
        """Replicate session (token, username) to all followers asynchronously with timeouts."""
        success_count = 0
        majority_count = self.get_majority_count()-1 #//paarth
        lock = threading.Lock()
        condition = threading.Condition(lock)

        def replicate_to_follower(follower_address):
            nonlocal success_count
            try:
                with grpc.insecure_channel(follower_address) as channel:
                    stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
                    response = stub.AddSession(
                        lms_pb2.AddSessionRequest(token=token, username=username),
                        timeout=5  # Set the RPC timeout here
                    )
                    if response.success:
                        with lock:
                            success_count += 1
                            condition.notify_all()
            except grpc.RpcError as e:
                logging.error(f"Failed to replicate session to follower {follower_address}: {e.details()}", extra={'node_id': self.node_id})

        threads = []
        for node_id, address in self.node_addresses.items():
            if node_id != self.node_id:
                follower_address = f"{address['ip']}:{address['port']}"
                thread = threading.Thread(target=replicate_to_follower, args=(follower_address,))
                thread.start()
                threads.append(thread)

        # Wait until majority is achieved or all threads are done
        with condition:
            while success_count < majority_count and any(thread.is_alive() for thread in threads):
                condition.wait(timeout=5)

        # Optionally, you can join all threads to ensure they're completed
        for thread in threads:
            thread.join(timeout=0)

        return success_count >= majority_count



    def replicate_data_store_to_followers(self, data_type, data_list):
        """Replicate the data_store to all followers after log replication."""
        success_count = 0
        follower_addresses = []

        for node_id, address in self.node_addresses.items():
            if node_id != self.node_id:  # Skip the leader itself
                ip = address['ip']
                port = address['port']
                follower_addresses.append(f"{ip}:{port}")

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
                ), timeout=5)

                if response.success:
                    success_count += 1
                # else:
                #     logging.warning(f"Data replication failed to {follower}", extra={'node_id': self.node_id})
            except grpc.RpcError as e:
                logging.error(f"Failed to replicate data store to follower {follower}", extra={'node_id': self.node_id})

        # Check if the majority of followers replicated the data_store
        return success_count >= self.get_majority_count()-1 #//paarth

    def AddSession(self, request, context):
        """Add session (token and username) received from the leader."""
        self.sessions[request.token] = request.username
        logging.info(f"User {request.username} with Token {request.token} is currently active with leader {self.leader_id}.", extra={'node_id': self.node_id})
        return lms_pb2.StatusResponse(success=True)

    def get_majority_count(self):
        total_nodes = len(self.node_addresses)
        return (total_nodes // 2) + 1

    def get_active_nodes(self):
        active_nodes = 0

        for node_id, address in self.node_addresses.items():
            ip = address['ip']
            port = address['port']
            try:
                # Try to establish a connection to the node
                channel = grpc.insecure_channel(f"{ip}:{port}")
                stub = lms_pb2_grpc.LMSRaftServiceStub(channel)

                # Use a lightweight RPC method to test if the node is alive, such as GetLeader
                response = stub.GetLeader(lms_pb2.GetLeaderRequest())

                # If we receive a response, consider the node active
                if response:
                    active_nodes += 1
            except grpc.RpcError as e:
                # Node is not available, log the error and continue
                logging.debug(f"Node {node_id} at {ip}:{port} is down or not responding", extra={'node_id': self.node_id})

        logging.info(f"Number of Current Active nodes: {active_nodes}", extra={'node_id': self.node_id})
        return active_nodes

    def ReplicateDataStore(self, request, context):
        if request.type not in self.data_store:
            self.data_store[request.type] = []
        self.data_store[request.type] = list(request.data)
        # logging.info(f"Updated data store: {self.data_store}", extra={'node_id': self.node_id})
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
            logging.info(f"Data store saved successfully.", extra={'node_id': self.node_id})
        except Exception as e:
            logging.error(f"Error saving data store", extra={'node_id': self.node_id})

    def load_data_store(self):
        """Load the data_store from a file, if it exists."""
        file_name = f"data_store_{self.node_id}.json"
        if os.path.exists(file_name):
            try:
                with open(file_name, 'r') as file:
                    data = json.load(file)
                    if isinstance(data, dict):
                        self.data_store = data
                    else:
                        logging.warning("data_store.json did not contain a dictionary. Initializing as empty dictionary.", extra={'node_id': self.node_id})
                        self.data_store = {}
                logging.info(f"Data store loaded successfully.", extra={'node_id': self.node_id})
            except Exception as e:
                logging.error(f"Error loading data store", extra={'node_id': self.node_id})
                self.data_store = {}  # Initialize an empty data store on error
        else:
            logging.info("No previous data store found, starting with an empty data store.", extra={'node_id': self.node_id})
            self.data_store = {}

    def get_leader_address(self, max_retries=10, retry_delay=2):
        """Retrieve the address of the current leader node, with retry logic."""
        for attempt in range(max_retries):
            try:
                if self.leader_id == -1:
                    raise Exception("Leader is not known yet.")

                # Retrieve leader IP and port from node_addresses
                if self.leader_id in self.node_addresses:
                    leader_info = self.node_addresses[self.leader_id]
                    leader_ip = leader_info['ip']
                    leader_port = leader_info['port']
                    leader_address = f"{leader_ip}:{leader_port}"
                    return leader_address
                else:
                    raise Exception(f"Leader ID {self.leader_id} is not recognized.")

            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...", extra={'node_id': self.node_id})
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Optional: exponential backoff
                else:
                    logging.error(f"All {max_retries} attempts failed. Error", extra={'node_id': self.node_id})
                    raise  # Re-raise the exception after all retries are exhausted

def serve(node_id, ip, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    raft_servicer = LMSRaftServiceServicer(node_id, port)

    lms_pb2_grpc.add_LMSRaftServiceServicer_to_server(raft_servicer, server)
    server.add_insecure_port(f'{ip}:{port}')
    server.start()

    logging.info(f"LMS Raft-based Server started on {ip}:{port}.", extra={'node_id': node_id})

    # After starting the server, check if the node is a follower
    if raft_servicer.state == FOLLOWER:
        logging.info("Node is a follower. Fetching data_store from local files.", extra={'node_id': node_id})
        raft_servicer.load_data_store()
        # Optionally fetch data_store from the leader if necessary
        # if not raft_servicer.data_store:
        #     logging.info("Node is a follower. Fetching data_store from leader.", extra={'node_id': node_id})
        #     raft_servicer.fetch_data_store_from_leader()  # Uncomment to fetch from leader

    try:
        while True:
            time.sleep(86400)  # Keep server running
    except KeyboardInterrupt:
        logging.info("Shutting down the server.", extra={'node_id': node_id})
        raft_servicer.save_data_store()  # Save the data store before shutting down
        server.stop(0)

if __name__ == '__main__':
    import sys

    if len(sys.argv) != 4:
        print("Usage: python raft_server.py <node_id> <ip> <port>")
        sys.exit(1)

    node_id = int(sys.argv[1])
    ip = sys.argv[2]
    port = int(sys.argv[3])

    serve(node_id, ip, port)
