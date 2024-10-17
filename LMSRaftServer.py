import grpc
from concurrent import futures
import time
import threading
import random
import lms_pb2
import lms_pb2_grpc

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
ELECTION_TIMEOUT = random.uniform(7,8)  # Election timeout range
HEARTBEAT_INTERVAL = 5  # Leader heartbeat interval

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
        self.data_store = {}  # Store assignments and other data by type

        self.port = port
        self.node_ports = {
            'node1': '50051',
            'node2': '50052',
            'node3': '50053',
            'node4': '50054'
        }

        # Start the election timer
        self.reset_election_timer()

    ### LMS Functionality (Login, Post, Get) ###
    
    def Login(self, request, context):
        """Handle user login with username and password."""
        if request.username in users and users[request.username] == request.password:
            token = f"token-{time.time()}"  # Generate a simple token
            self.sessions[token] = request.username
            # return lms_pb2.LoginResponse(success=True, token=token, )
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
                self.data_store[request.type] = []
            self.data_store[request.type].append(request.data)
            log_entry = {'term': self.current_term, 'type': request.type, 'data': request.data}
            self.log.append(log_entry)
            print(self.log)
            # Send AppendEntries RPC to all followers
            # success = self.replicate_to_followers(log_entry)
            # if success:
            if True:
                # After log replication, send the data_store update
                if self.replicate_data_store_to_followers(request.type, self.data_store[request.type]):
                    return lms_pb2.StatusResponse(success=True, message="Post replicated and committed")
                else:
                    return lms_pb2.StatusResponse(success=False, message="Log replicated but failed to replicate data store")
                return lms_pb2.StatusResponse(success=True, message="Post replicated and commited")
            else:
                print("Failed")
                return lms_pb2.StatusResponse(success=False, message="Failed to replicate log to majority")
        return lms_pb2.StatusResponse(success=False, message="Invalid token")

    def Get(self, request, context):
        """Handle getting data."""
        if request.token in self.sessions:
            data = self.data_store.get(request.type, [])
           # data = self.log[self.get_last_log_index()].get(request.type,[])
            data_items = [lms_pb2.DataItem(type_id=str(i), data=item) for i, item in enumerate(data)]
            return lms_pb2.GetResponse(data=data_items)
        return lms_pb2.GetResponse(data=[])

    ### Raft Functionality (Leader Election, Heartbeats) ###

    def reset_election_timer(self):
        """Reset election timer and convert to candidate if timeout occurs."""
        print(f"Node {self.node_id} resetting election timer.")
        if self.election_timer:
            self.election_timer.cancel()
        self.election_timer = threading.Timer(ELECTION_TIMEOUT, self.start_election)
        self.election_timer.start()

    def start_election(self):
        """Convert to candidate and start a new election."""
        print(f"Node {self.node_id} is starting an election for term {self.current_term + 1}.")
        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1  # Vote for self

        # Send RequestVote RPCs to all other nodes
        for node, port in self.node_ports.items():
            if node != self.node_id:
                request_vote_request = lms_pb2.RequestVoteRequest(
                    candidateId=self.node_id,
                    term=self.current_term,
                    lastLogIndex=len(self.log) - 1,
                    lastLogTerm=self.log[-1]['term'] if self.log else 0
                )
                self.send_request_vote(port, request_vote_request)

    def send_request_vote(self, port, request_vote_request):
        """Send RequestVote RPC to another node."""
        try:
            print(f"Node {self.node_id} sending RequestVote to port {port}")
            channel = grpc.insecure_channel(f'localhost:{port}')
            stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
            response = stub.RequestVote(request_vote_request)
            print(f"Node {self.node_id} received response from port {port}")
            self.handle_vote_response(response)
        except Exception as e:
            print(f"Error sending RequestVote to port {port}: {e}")
            # print(f"Node unreachable at port {port}")

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
            if self.votes_received > (len(self.node_ports) // 2):
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
                    self.send_append_entries(node, port, append_entries_request)
            self.reset_heartbeat_timer()

    def send_append_entries(self, node, port, append_entries_request):
        """Send AppendEntries RPC to a follower."""
        try:
            channel = grpc.insecure_channel(f'localhost:{port}')
            stub = lms_pb2_grpc.LMSRaftServiceStub(channel)
            response = stub.AppendEntries(append_entries_request)
            self.handle_append_entries_response(response)
        except Exception as e:
            print(f"Error sending AppendEntries to {port}: {e}")
            # print(f"Node unreachable at port {port}")

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
            # Check if the term in the request is valid
        if request.term < self.current_term:
            return lms_pb2.AppendEntriesReply(term=self.current_term, success=False)
        
        self.current_term = request.term
        self.leader_id = request.leaderId
        self.reset_election_timer()  # Reset election timer since we've heard from the leader
        print(f"Node {self.node_id} received heartbeat from leader {self.leader_id}.")
    
        # Append the log entry to the follower's log
        if request.entries:
            self.log.append(request.entries[0])  # Assume single entry for simplicity

            # Update the follower's data_store based on the received log entry
            if request.data_type not in self.data_store:
                self.data_store[request.data_type] = []
        
            # Append the data received from the leader to the follower's data_store
            self.data_store[request.data_type].append(request.data)
    
        # Update the commit index on the follower
        if request.leaderCommit > self.commit_index:
            self.commit_index = min(request.leaderCommit, len(self.log) - 1)
    
        # Respond with success
        return lms_pb2.AppendEntriesReply(term=self.current_term, success=True)


        # if request.term < self.current_term:
        #     return lms_pb2.AppendEntriesReply(term=self.current_term, success=False)
        # self.current_term = request.term
        # self.leader_id = request.leaderId
        # self.reset_election_timer()  # Reset election timer since we've heard from the leader
        # print(f"Node {self.node_id} received heartbeat from leader {self.leader_id}.")
        
        # if len(request.entries) > 0:
        #     for entry in request.entries:
        #         self.log.append(entry)
        #     # Acknowledge successful replication
        #     return lms_pb2.AppendEntriesReply(term=self.current_term, success=True)
        # return lms_pb2.AppendEntriesReply(term=self.current_term, success=True)


        # if request.prevLogIndex == -1 or request.prevLogIndex >= len(self.log):
        # # Handle case where follower has an empty log or insufficient log entries
        #     return lms_pb2.AppendEntriesReply(term=self.current_term, success=True)

        # if request.term < self.current_term:
        #     return lms_pb2.AppendEntriesReply(term=self.current_term, success=False)

        # # If log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm, reject
        # if request.prevLogIndex >= len(self.log):
        #     print(f"Follower log too short: prevLogIndex = {request.prevLogIndex}, log length = {len(self.log)}")
        #     return lms_pb2.AppendEntriesReply(term=self.current_term, success=False)
        
        # if request.prevLogIndex >= 0 and self.log[request.prevLogIndex]['term'] != request.prevLogTerm:
        #     # The term at prevLogIndex does not match, reject the append
        #     return lms_pb2.AppendEntriesReply(term=self.current_term, success=False)

        # # Append any new entries not already in the log
        # if len(request.entries) > 0:
        #     self.log = self.log[:request.prevLogIndex + 1]  # Delete conflicting entries
        #     self.log.extend(request.entries)  # Append the new entries

        # # Update commit index
        # if request.leaderCommit > self.commit_index:
        #     self.commit_index = min(request.leaderCommit, len(self.log) - 1)

        # return lms_pb2.AppendEntriesReply(term=self.current_term, success=True)
    
    def replicate_to_followers(self, log_entry):
        success_count = 0
        follower_addresses = []
        for node, port in self.node_ports.items():
            if node != self.node_id:
                follower_addresses.append("localhost:"+port)
        for follower in follower_addresses:
            try:
                # Create a gRPC stub for the follower
                channel = grpc.insecure_channel(follower)
                stub = lms_pb2_grpc.LMSRaftServiceStub(channel)

                # Get the data that needs to be replicated from the leader's data_store
                data_type = log_entry['type']
                data = log_entry['data']
                
                # Send the AppendEntries RPC to replicate the log entry and the data store update
                response = stub.AppendEntries(lms_pb2.AppendEntriesRequest(
                    leaderId=self.leader_id,
                    term=self.current_term,
                    prevLogIndex=self.get_last_log_index(),
                    prevLogTerm=self.get_last_log_term(),
                    entries=[log_entry],  # Sending the new log entry to the follower
                    leaderCommit=self.commit_index,
                    # Adding the data_store update to the AppendEntriesRequest
                    data_type=data_type,
                    data=data
            ))
                
                if response.success:
                    success_count += 1
                else:
                    print(f"Replication failed to {follower} with term {response.term}")

            except grpc.RpcError as e:
                print(f"Failed to replicate to follower {follower}: {e}")
            

        # Return True if the majority of followers replicated the entry
        # If the majority of followers successfully replicated the log entry, commit it
        if success_count >= self.get_majority_count():
            self.commit_log_entry(log_entry)  # <-- Call commit_log_entry here
            return True
        
        return False
    
    def commit_log_entry(self, log_entry):
        # Increment the commit index
        self.commit_index += 1
        # Mark the log entry as committed
        if log_entry['type'] not in self.data_store:
            self.data_store[log_entry['type']] = []
        self.data_store[log_entry['type']].append(log_entry['data'])
        print(f"Committed log entry: {log_entry}")
        # log_entry['committed'] = True
        # print(f"Log entry committed: {log_entry}")
    
    def GetLeader(self, request, context):
        """Respond with whether this node is the leader, and the leader's ID."""
        if self.state == LEADER:
            return lms_pb2.GetLeaderReply(isLeader=True, leaderId=self.node_id)
        else:
            return lms_pb2.GetLeaderReply(isLeader=False, leaderId=self.leader_id if self.leader_id else "")
    
    def get_last_log_index(self):
        # If log is empty, return 0 (no log entries yet)
        if len(self.log) == 0:
            return 0
        # The last index is simply the length of the log, since log indices are 1-based
        return len(self.log) - 1
    
    def get_last_log_term(self):
        # If log is empty, return current term (or a default term, e.g., 0)
        if len(self.log) == 0:
            return self.current_term  # Return the current term if no log entries
        # Return the term of the last log entry
        return self.log[-1]['term']  # Assuming log entries are stored as dicts with 'term' and 'data'
    
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
                print(f"Failed to replicate session to follower {follower}: {e}")
    
        # Return True if a majority of followers successfully replicated the session
        return success_count >= self.get_majority_count()
    
    def AddSession(self, request, context):
        """Add session (token and username) received from the leader."""
        # Replicate the session locally on the follower
        self.sessions[request.token] = request.username
        print(f"Token {request.token} added. User {request.username} logged in")
        return lms_pb2.StatusResponse(success=True)
    
    def get_majority_count(self):
        """Calculate and return the majority count of the nodes in the cluster."""
        total_nodes = len(self.node_ports)  # `node_ports` should contain all the nodes in the cluster
        return (total_nodes // 2) + 1  # Majority is half the nodes + 1
    
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
                    ))
            
                    if response.success:
                        success_count += 1
                    else:
                        print(f"Data replication failed to {follower} with message: {response.message}")

                except grpc.RpcError as e:
                    print(f"Failed to replicate data store to follower {follower}: {e}")
    
        # Check if the majority of followers replicated the data_store
        return success_count >= self.get_majority_count()
    
    def ReplicateDataStore(self, request, context):
        """Handle replication of data store from the leader."""
        if request.type not in self.data_store:
            self.data_store[request.type] = []
    
        # Update the data_store with the data received from the leader
        self.data_store[request.type] = list(request.data)  # Replace with the leader's version of the data

        print(self.data_store)
    
        return lms_pb2.StatusResponse(success=True, message="Data store replicated successfully")





def serve(node_id, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lms_pb2_grpc.add_LMSRaftServiceServicer_to_server(LMSRaftServiceServicer(node_id, port), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"LMS Raft-based Server {node_id} started on port {port}.")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print(f"Shutting down the server {node_id}.")
        server.stop(0)


if __name__ == '__main__':
    import sys

    if len(sys.argv) != 3:
        print("Usage: python raft_server.py <node_id> <port>")
        sys.exit(1)

    node_id = sys.argv[1]
    port = sys.argv[2]

    serve(node_id, port)
