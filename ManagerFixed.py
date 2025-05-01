import socket as socket
import sys
import random
from threading import Lock
from collections import defaultdict;

"""
Implementtion of an Always On Manager. Supports the management of peers implementing the DHT and other functionality
Reads
    One command line parameter
    Integer giving port number at which server listens
    Messages / commands


Stores in an state information base
    Name
    IPv4 Address
    2 ports associated with that peer (M Port for manager2peer, P Port for peer2peer)

"""
class DHTManager:
    """
    Process started on a host with provided port number
    Runs on an infinite loop, listening to messages on given poprt from peers
    Manager maintains state of the DHT application, including state of peers
    """
    def __init__(self, port):
        self.port = port
        self.peers = {}     # container for peers {ip, ports, state}
        self.dht_leader = None
        self.dht_members = set()
        self.dht_created = False
        self.waiting_for = None     # waits for 'dht_complete', 'dht_rebuilt', etc
        self.pending_peer = None    # used for leave/joinDHT
        self.lock = Lock()

        return

    """
    Registers a peer with the manager. All peers must be registered prior to issuing other commands to amanger
    Manager stores:
        Name (Must be Unique)
        IPv4 Address (Need not be unique)
        2 Ports associated with peer (Must be unique)
    State of peer is set to FREE
    """
    def register(self, peer_name, addr, m_port, p_port):
        with self.lock:
            if peer_name in self.peers:
                return "FAILURE: Duplicate Peer Name"
            for peer in self.peers.values():
                if peer['m_port'] == m_port or peer['p_port'] == p_port:
                    return "FAILURE: Port conflict"
            self.peers[peer_name] = {
                'ip': addr,
                'm_port': m_port,
                'p_port': p_port,
                'state': 'Free'
                }
            return "SUCCESS"

    """
    Initiates construction of DHT of size n using data from year year, with peer_name as its leader

    Succeeds if
        Only one DHT can exist at a time
        n must be greater than 3
        peer_name is registered

    Manager sets state of peer_name to LEADER
    Selects random n-1 FREE registered users, updates state to INDHT
    list of n peers constructs the DHT

    Waits for completeDHT before responding to any other messages
    """
    def setupDHT(self, peer_name, n, year):
        with self.lock:
            # handle errors
            if self.dht_created:
                return "FAILURE: DHT exists"
            if peer_name not in self.peers:
                return "FAILURE: Peer not registered"
            if n < 3:
                return "FILURE: Number of peers is less than 3"

            # find free peers
            free_peers = [name for name, info in self.peers.items() if info['state'] == 'Free']
            print(free_peers)
            if len(free_peers) < n:
                return "FAILURE: Not enough free peers"
            if self.peers[peer_name]['state'] != 'Free':
                return "FAILURE: Leader not free"
            
            # ok to select random peers and set up DHT
            selected = [peer_name] + random.sample([p for p in free_peers if p != peer_name], n-1)
            self.dht_leader = peer_name
            self.dht_members = set(selected)
            for name in selected:
                self.peers[name]['state'] = 'Leader' if name == peer_name else 'InDHT'
            self.dht_created = True
            self.waiting_for = 'dht-complete'
            response = "SUCCESS"
            for name in selected:
                peer = self.peers[name]
                response += f" {name} {peer['ip']} {peer['p_port']}"
            return response

    """
    Receipt of completeDHT indicates leader has completed all steps to set up DHT
    Allows manager to process any other command
    """
    def completeDHT(self, peer_name):
        with self.lock:
            if not self.dht_created or self.waiting_for != 'dht-complete':
                return "FAILURE"
            if peer_name != self.dht_leader:
                return "FAILURE. Not leader"
            self.waiting_for = None
            return "SUCCESS"

    """
    Used to initiate query of DHT
    Manager chooses at random one of n peers maintaining DHT and responds with 3-Tuple:
        peer_name
        ipv4 addr
        peer port

    Fails if
        DHT set up has not been completed
        peer_name not registered
        peer_name is registered but not FREE

    INVOLVES MORE FROM THE PEER
    """
    # TO DO: NEED TO ADD MORE PARAMETERS HERE
    def queryDHT(self, peer_name):
        with self.lock:
            if not self.dht_created or self.waiting_for is not None:
                return "FAILURE. DHT not ready"
            if peer_name not in self.peers or self.peers[peer_name]['state'] != 'Free':
                return "FAILURE. Invalid Peer"
            dht_members = list(self.dht_members)
            if not dht_members:
                return "FAILURE. No DHT members"

            selected = random.choice(dht_members)
            peer = self.peers[selected]
            return f"SUCCESS {selected} {peer['ip']} {peer['p_port']}"

    """
    Initiates process of peer_name leaving the DHT
    Manager stores peer_name making the request
    Server waits for rebuiltDHT, returning FAILURE for any other messages

    Fails if
        peer is not maintaining the DHT
        DHT does not exist
    
    INVOLVES MORE FROM THE PEER
    """
    def leaveDHT(self, peer_name):
        if not self.dht_created or peer_name not in self.dht_members:
            return "FAILURE"
        self.pending_peer = peer_name
        self.waiting_for = 'dht-rebuilt'
        return "SUCCESS"

    """
    Initaites process of peer_name joining the DHT
    Manager stores peer_name making request, waiting for rebuiltDHT, returning failure to other messages

    Fails if
        peer_name is not FREE
        DHT does not exist

    INVOLVES MORE FROM THE PEER
    """
    def joinDHT(self, peer_name):
        with self.lock:
            if not self.dht_created or self.peers[peer_name]['state'] != 'Free':
                return "FAILURE"
            self.pending_peer = peer_name
            self.waiting_for = 'dht-rebuilt'
            return "SUCCESS"

    """
    Receipt of rebuiltDHT indicates all steps of managing the peer churn ahve been completed
    if peer_name is not same as the stored initaiting peer_name, returns FAILURE
    Manager sets state of peer_name as appropriate, and may require assignming of new leader
    SETS STATES ACCORDINGLY
    """
    def rebuiltDHT(self, peer_name, new_leader):
        with self.lock:
            if self.waiting_for != 'dht-rebuilt' or peer_name  != self.pending_peer:
                return "FAILURE"
            self.dht_leader = new_leader
            self.waiting_for = None
            self.pending_peer = None
            return "SUCCESS"

    """
    Removes state of a FREE peer from state information base, allowing it to termiante
    Peer's state information is removed, manager responds with SUCCESS, uer process exits

    Fails if
        peer_name state is INDHT
    """
    def deregister(self, peer_name):
        with self.lock:
            if peer_name not in self.peers or self.peers[peer_name]['state'] == 'InDHT':
                return "FAILURE"   
            del self.peers[peer_name]
            return "SUCCESS"

    """
    Initiates deletion of DHT
    Manager waits for teardownComplete, returning FAILURE to any other message
    
    Fails if
        peer_name is not leader of DHT
    """
    def teardownDHT(self, peer_name):
        with self.lock:
            if not self.dht_created or peer_name != self.dht_leader:
                return "FAILURE"
            self.waiting_for = 'teardown-complete'
            return "SUCCESS"

    """
    Indicates DHT has been deleted
    Manager chages state of each peer involved in maintaining the DHT to Free

    Fails if
        peer_name is not leader of DHT
    """
    def teardownComplete(self, peer_name):
        with self.lock:
            if not self.dht_created or self.waiting_for != 'teardown-complete' or peer_name != self.dht_leader:
                return "FAILURE"
            for name in self.dht_members:
                self.peers[name]['state'] = 'Free'
            self.dht_created = False
            self.dht_members = set()
            self.dht_leader = None
            self.waiting_for = None
            return "SUCCESS"

    """
    Decides which command from a received message to run
    """
    def process_command (self, data):
        parts = data.split()
        if not parts:
            return "FAILURE. Empty command"
        cmd = parts[0]
        try:
            if cmd == 'register':
                if len(parts) != 5:
                    return "FAILURE. Invalid register command"
                return self.register(parts[1], parts[2], int(parts[3]), int(parts[4]))
            elif cmd == 'deregister':
                if len(parts) != 2:
                    return "FAILURE"
                return self.deregister(parts[1])
            elif cmd == 'setup-dht':
                if len(parts) != 4:
                    return "FAILURE. Invalid setup-dht command"
                return self.setupDHT(parts[1], int(parts[2]), parts[3])
            elif cmd == 'dht-complete':
                if len(parts) != 2:
                    return "FAILURE"
                return self.completeDHT(parts[1])
            elif cmd == 'query-dht':
                if len(parts) != 2:
                    return "FAILURE"
                return self.queryDHT(parts[1])
            elif cmd == 'leave-dht':
                if len(parts) != 2:
                    return "FAILURE"
                return self.leaveDHT(parts[1])
            elif cmd == 'join-dht':
                if len(parts) != 2:
                    return "FAILURE"
                return self.joinDHT(parts[1])
            elif cmd == 'dht-rebuilt':
                if len(parts) != 3:
                    return "FAILURE"
                return self.rebuiltDHT(parts[1], parts[2])
            elif cmd == 'teardown-dht':
                if len(parts) != 2:
                    return "FAILURE"
                return self.teardownDHT(parts[1])
            elif cmd == 'teardown-complete':
                if len(parts) != 2:
                    return "FAILURE"
                return self.teardownComplete(parts[1])
            else:
                return "FAILURE. Unknown Command"
        except ValueError:
            return "FAILURE. Invalid Parameters"

    """
    Starts the manager process and creates sockets
    """
    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", self.port))
        print(f"Manager running on port {self.port}")
        while True:
            data, addr = sock.recvfrom(1024)
            response = self.process_command(data.decode())
            sock.sendto(response.encode(), addr)

if __name__ == '__main__':
    print("running?")
    if len(sys.argv) != 2:
        print("Usage: python dht_manager.py <port>")
        sys.exit(1)
    manager = DHTManager(int(sys.argv[1]))
    manager.run()


