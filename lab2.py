"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Aditya Ganti
:Assignment: Lab 2
:EC PROBE Message: Claiming
:EC Feigning Failure: Not attempted
"""
import logging
from datetime import datetime
import pickle
import queue
import random
import socket
import socketserver
import sys
import threading
import time


class Node:
    """Represents a node in the distributed system. Each node has a unique identity,
    listens for incoming messages, and participates in leader election processes.
    """
    # Class Variables
    BULLY = None  # To store the leader info
    ELECTION_IN_PROGRESS = False  # To check whether the current node is in an election
    RECEIVED_ELECTION_RESPONSE_OK = False  # To check whether the node received an OK during the election
    GROUP_MEMBERS = None  # To keep track of all the members in the group
    HAS_ELECTION_TIMED_OUT = True

    def __init__(self, node_identity, gcd_address):
        self.identity = (self.days_until_next_birthday(node_identity[1]), int(node_identity[0]))
        self.listen_address = ('127.0.0.1', 0)
        print(f"\nThis node's identity -> Days to birthday: {self.identity[0]}, SU ID: {self.identity[1]}")

        self.gcd_hostname, self.gcd_port = gcd_address
        self.GROUP_MEMBERS = {}  # Dictionary to store other nodes and their addresses
        self.response_queue = queue.Queue()

        self.lock = threading.Lock()
        self.all_threads_done = threading.Event()
        self.threads = []

        # Start a separate thread for the node to connect to the GCD
        join_group_thread = threading.Thread(target=self.join_group_via_gcd)
        join_group_thread.daemon = True
        print(f"Thread spawned for connecting to the GCD")
        join_group_thread.start()

        # Start a threaded TCP server for the node to listen for incoming messages
        self.server = socketserver.ThreadingTCPServer(
            self.listen_address, lambda request, client_address, server: MessageHandler(
                request, client_address, server, self
            )
        )
        self.server.node = self
        server_thread = threading.Thread(target=self.server.serve_forever)
        server_thread.daemon = False
        self.listen_address = ('localhost', self.server.server_address[1])
        print(f"Thread spawned for server. The listening address for this node is: {self.listen_address}")
        server_thread.start()

        # Start a separate thread for the node to keep probing the bully
        self.probe_handler = ProbeHandler(self)
        probe_thread = threading.Thread(target=self.probe_handler.send_probe_message())
        probe_thread.daemon = False
        print(f"Thread spawned for sending PROBE messages")
        probe_thread.start()

    @staticmethod
    def days_until_next_birthday(mother_birthdate):
        """Helper function that checks the input mother's birthday, ensures a correct format,
        and returns the number of days from the current date to the birthday

        Args:
            mother_birthdate (string): Mother's birthday in YYYY-MM-DD format

        Raises:
            ValueError: Exception raised if the date format is incorrect

        Returns:
            int: Number of days to reach the next birthday, from the current date
        """

        # Parse the input date string to a datetime object
        try:
            mother_birthdate = datetime.strptime(mother_birthdate, "%Y-%m-%d")

        except ValueError:
            #raise ValueError("Incorrect date format. Please enter the date in YYYY-MM-DD format.")
            print("Incorrect date format. Please enter the date in YYYY-MM-DD format.")
            sys.exit(0)

        else:
            today = datetime.today()
            next_birthday = datetime(today.year, mother_birthdate.month, mother_birthdate.day)

            if today > next_birthday:
                next_birthday = datetime(today.year + 1, mother_birthdate.month, mother_birthdate.day)

            days_until_birthday = (next_birthday - today).days
            return days_until_birthday

    def highest_identity_member(self):
        """Helper function to find the member with the highest identity in the group

        Returns:
            tuple: The (days_to_bday, su_id) pair of the member with the highest ID
        """

        highest_identity = (0,0)
        for member_id, member_address in self.GROUP_MEMBERS.items():
            if self.is_higher_identity(highest_identity, member_id):
                highest_identity = member_id
        return highest_identity

    @staticmethod
    def is_higher_identity(node1, node2):
        """Helper function to check if node2's identity is higher than the current

        Args:
            node1 (tuple): The current node's process_id
            node2 (tuple): The other node's process_id

        Returns:
            bool: True if node2 has a higher process_id than node1
        """

        node1_bday, node1_id = node1
        node2_bday, node2_id = node2
        if node2_bday > node1_bday:
            return True
        if node2_bday == node1_bday:
            return True if node2_id > node1_id else False
        return False

    @staticmethod
    def generate_join_message_payload(days_until_bday, su_id, hostname, port):
        """Helper function to generate a JOIN message's payload for a node

        Args:
            days_until_bday (int): Number of days until your mother's birthday
            su_id (int): Your SeattleU student id
            hostname (string): The IP Address this node is running on
            port (int): The port number this node is running on

        Returns:
            tuple: A tuple of the form ((process_id), (listen_address))
        """

        return (days_until_bday % 365, su_id), (hostname, port)

    @staticmethod
    def generate_join_message(self, days_until_bday, su_id, hostname, port):
        """Generates the entire JOIN message required to communicate with the GCD

        Args:
            days_until_bday (int): Number of days until your mother's birthday
            su_id (int): Your SeattleU student id
            hostname (string): The IP Address this node is running on
            port (int): The port number this node is running on

        Returns:
            tuple: A tuple of the form ("JOIN", (join_message_payload))
        """

        return 'JOIN', self.generate_join_message_payload(days_until_bday, su_id, hostname, port)

    @staticmethod
    def generate_election_message(gcd_member_list):
        """Helper function to generate the entire ELECTION message

        Args:
            gcd_member_list (dict): A dictionary consisting of all the known members in the node's group

        Returns:
            tuple: A tuple of the form ("ELECTION", {(22, 1234567): ('127.0.0.1', 0)})
        """

        return 'ELECTION', gcd_member_list

    @staticmethod
    def generate_coordinator_message(new_leader, new_leader_address):
        """Helper function to generate the entire COORDINATOR message

        Args:
            new_leader (tuple): The process id of the new leader of the group
            new_leader_address (tuple): The listening address of the new leader of the group

        Returns:
            tuple: A tuple of the form ("COORDINATOR", {(22, 1234567): ('127.0.0.1', 0)})
        """

        return 'COORDINATOR', {new_leader: new_leader_address}

    @staticmethod
    def generate_probe_message():
        """Helper function to generate the entire PROBE message

        Returns:
            tuple: A tuple of the form ("PROBE", None)
        """

        return 'PROBE', None

    def join_group_via_gcd(self):
        """This function is responsible for creating a connection to the Group Coordinator Daemon
        """
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Might have to move this into init
        print(f"\n\n{self.identity} is going to join the group. Connecting to the GCD.")
        try:
            self.socket.connect((self.gcd_hostname, self.gcd_port))  # Connecting to the GCD
        except Exception as e:
            print(f"An error occurred trying to connect to the Daemon: {str(e)}")
        else:
            self.get_member_list_from_gcd()

    def get_member_list_from_gcd(self):
        """This function is responsible for sending a JOIN message to the GCD. It receives a list of
        group members in return, and proceeds to the 'In Election State'
        """

        process_id = (self.identity, self.listen_address)
        print(f"Sending the join message to the GCD -> ('JOIN', {process_id})")
        self.socket.sendall(pickle.dumps(('JOIN', process_id)))  # Sending a JOIN message
        self.GROUP_MEMBERS = pickle.loads(self.socket.recv(1024))
        print(f"Here is the list of members returned by the GCD:\n{self.GROUP_MEMBERS}\n")
        self.socket.close()

        print("In the startup phase. Calling for the mandatory election while joining")
        print(f"Current leader is: unknown")
        self.in_election_state()

    def send_election_message(self, member_identity, member_address):
        """Socket-Level function that is responsible for sending an ELECTION message to a group member

        Args:
            member_identity (tuple): The process id of the group member
            member_address (tuple): The listening address of the group member
        """

        # Implement logic to create a socket and send "VOTE" message
        election_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        election_socket.settimeout(2)
        try:
            election_socket.connect(member_address)  # Connecting to the member
        except Exception as e:
            #logging.error(f"Couldn't connect to the member at {member_address} to send an ELECTION message: {str(e)}")
            print()
        else:
            election_socket.settimeout(2)
            try:
                election_socket.sendall(pickle.dumps(self.generate_election_message(self.GROUP_MEMBERS)))  # Sending "ELECTION" message
            except Exception as ex:
                #logging.error(f"Timed out trying to connect to the member at {member_address}: {str(ex)}")
                self.all_threads_done.set()
            else:
                response = pickle.loads(election_socket.recv(1024))
                #print(f"Received ELECTION response from {member_address}: ", response)
                if response == "OK":
                    with self.lock:
                        self.HAS_ELECTION_TIMED_OUT = False
                    with self.lock:
                        self.RECEIVED_ELECTION_RESPONSE_OK = True
                    if self.is_higher_identity(self.BULLY, member_identity):
                        with self.lock:
                            self.BULLY = member_identity
                    election_socket.close()
                    self.all_threads_done.set()

    def send_coordinator_message(self, new_bully_id, new_bully_address, member_address):
        """Socket-Level function that is responsible for sending a COORDINATOR message to a group member

        Args:
            new_bully_id (tuple): A tuple that represents the new leader's process id
            new_bully_address (tuple): A tuple that represents the new leader's listening address
            member_address (tuple): A tuple that holds the group member's IP Address and Port number
        """

        # Implement logic to create a socket and send "COORDINATOR" message
        coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            coordinator_socket.connect(member_address)  # Connecting to the member
        except Exception as e:
            print(f"Failed to send COORD message to {member_address}: {str(e)}")
        else:
            coordinator_socket.sendall(pickle.dumps(self.generate_coordinator_message(new_bully_id, new_bully_address)))  # Sending "COORDINATOR" message
            print("Successfully sent COORD message to ", member_address)
        finally:
            coordinator_socket.close()

    def in_leader_state(self, new_bully, new_bully_address):
        """This function represents the node's 'Leader' state. It spawns threads for all the group members that
        have a lower process_id than the node in order to send COORDINATOR messages. After that, it does nothing

        Args:
            new_bully (tuple): The process id of the new leader of the group
            new_bully_address (tuple): The listening address of the new leader of the group
        """

        print(f"\n{new_bully} is in the leader state now")
        for member_id, member_address in self.GROUP_MEMBERS.items():
            if not self.is_higher_identity(self.identity, member_id):
                print("Sending COORD message all lower-ranked members")
                if not member_id == self.identity:
                    threading.Thread(target=self.send_coordinator_message, args=(new_bully, new_bully_address, member_address,)).start()
        print("Finished sending out all the COORD messages\n")

    def in_election_state(self):
        """This function represents the node's state of being in an election. It loops through all the
        members present in the group, and spawns corresponding threads. If the election times out, it
        moves to the 'In Leader State'. It keeps track of any OK messages received as responses to the
        ELECTION messages as well. If the node didn't win the ELECTION, it simply moves to the 'Idle' state
        """

        print(f"Node {self.identity} is in the election state")
        if self.identity == self.highest_identity_member():
            print(f"\n{self.identity} has the highest id in the member group. Declaring self the leader.")
            self.in_idle_state()
        else:
            print(f"\n{self.identity} is in the election state. Starting the election process.")
            self.HAS_ELECTION_TIMED_OUT = True
            for member_id, member_address in self.GROUP_MEMBERS.items():
                if self.is_higher_identity(self.identity, member_id):
                    print(f"Sending ELECTION message to higher-ranked member -> {member_id} at {member_address}")
                    # Start a new thread for each node that has a higher process_id
                    thread = threading.Thread(target=self.send_election_message, args=(member_id, member_address,))
                    thread.start()
                    self.threads.append(thread)

        self.ELECTION_IN_PROGRESS = False  # Signals the end of the election process
        print("\nElection complete")
        with self.lock:
            election_response_received = self.RECEIVED_ELECTION_RESPONSE_OK
            election_timed_out = self.HAS_ELECTION_TIMED_OUT
        print(f"Election Status: OK received from at least one member: {election_response_received}")

        # If an OK has been received, but no expected COORDINATE, then restart the election
        if election_response_received and election_timed_out:
            print("Was expecting a COORDINATE message, but didn't get one. Restarting the election.")
            with self.lock:
                self.RECEIVED_ELECTION_RESPONSE_OK = False
                self.HAS_ELECTION_TIMED_OUT = True
            self.in_election_state()

        # If the election has timed out, declare itself the leader
        if (not election_response_received) and election_timed_out:
            print(f"Election process timed out. New leader is {self.identity}")
            with self.lock:
                self.BULLY = self.identity
            self.in_leader_state(self.BULLY, self.GROUP_MEMBERS[self.BULLY])
        else:
            with self.lock:
                self.RECEIVED_ELECTION_RESPONSE_OK = False
                self.HAS_ELECTION_TIMED_OUT = True
            self.in_idle_state()

    @staticmethod
    def in_idle_state():
        """This function represents the node's idle state. It simply does nothing
        """

        print("\nThe node has now moved into an idle state. Listening for incoming messages...")

    def handle_election_message(self, new_member_list):
        """Handler function that is responsible for the actions taken after receiving an
        ELECTION message. It updates its member list with the received payload, and proceeds to
        start a new election, if it isn't in one already

        Args:
            new_member_list (dict): A dict that contains the list of group members
        """

        # Implement logic to start the election process
        with self.lock:
            self.GROUP_MEMBERS.update(new_member_list)  # Updating the member list to include any new members

        self.response_queue.put("OK")  # Adding an OK back to the response queue

        # Going to check if election is in progress. Going to start one if there's no ongoing election
        if self.ELECTION_IN_PROGRESS is not True:
            print("No election is currently in progress")
            with self.lock:
                self.ELECTION_IN_PROGRESS = True
            self.in_election_state()
        else:
            print(f"Election by {self.identity} is already in progress")

    def handle_coordinator_message(self, new_leader_identity):
        """Handler function that is responsible for the actions taken after receiving a
        COORDINATOR message. It updates its member list with the new leader, and notes
        the new leader's details

        Args:
            new_leader_identity (tuple): A tuple of the form (process_id, listening_address)
        """

        print("post COORD New leader is -> ", new_leader_identity)
        with self.lock:
            self.GROUP_MEMBERS.update(new_leader_identity)

        with self.lock:
            self.BULLY = new_leader_identity
        print("New Leader has been identified as -> ", self.BULLY)


class ProbeHandler:
    """Handles the logic for sending periodic probe messages to the leader node
    to ensure its liveliness.
    """
    def __init__(self, node):
        """Initializes a new ProbeHandler instance associated with the provided Node instance.

        Args:
            node_instance (Node): The Node instance to which this ProbeHandler is associated.
        """
        self.node = node
        self.running = True

    def stop(self):
        """Stops the probe message sending process.
        """
        self.running = False

    def send_probe_message(self):
        """Sends periodic probe messages to the leader node to check its liveliness.
        """
        while self.running:
            sleep_time = random.randint(500, 3000) / 1000  # Converting milliseconds to seconds
            time.sleep(sleep_time)  # Send a PROBE randomly
            probe_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.node.BULLY and self.node.BULLY != self.node.identity:
                new_leader_address = next(iter(self.node.BULLY.values()))  # Extracting the bully's socket address
                print(f"\nSending a PROBE message to the leader at: {new_leader_address}")
                try:
                    probe_socket.connect(new_leader_address)  # Connecting to the leader
                except Exception as e:
                    print(f"PROBE failed at timestamp : {datetime.now()} --> {str(e)}")
                    # Have to re-register with the GCD
                    self.node.join_group_via_gcd()
                else:
                    probe_socket.sendall(pickle.dumps(self.node.generate_probe_message()))  # Sending PROBE message
                    probe_response = pickle.loads(probe_socket.recv(1024))
                    print(f"PROBE passed at timestamp : {datetime.now()} --> {probe_response} received from the leader")
                    probe_socket.close()
            else:
                print(f"Not sending a PROBE, since I'm the leader myself. Timestamp : {datetime.now()}")


class MessageHandler(socketserver.BaseRequestHandler):
    """Handles incoming messages received by the Node from other nodes in the distributed system.
    """
    def __init__(self, request, client_address, server, node_instance):
        """Initializes a new MessageHandler instance with the provided parameters.

        Args:
            request (socket.socket): The socket connection request from a client node.
            client_address (tuple): The address of the client node (hostname, port).
            server (socketserver.BaseServer): The server instance handling the connection.
            node_instance (Node): The Node instance associated with this MessageHandler.
        """
        self.node = node_instance
        super().__init__(request, client_address, server)
        self.node = node_instance

    def handle(self):
        """Overrides the handle() function. This class runs on the TCPThreadingServer thread.
        It's always running, listening for incoming messages, and handles them accordingly
        """
        
        data = None
        while not data:
            data = pickle.loads(self.request.recv(4096))
        if data[0] == "ELECTION":
            print("\nReceived an ELECTION message")
            self.request.send(pickle.dumps("OK"))
            self.node.handle_election_message(data[1])

        elif data[0] == "COORDINATOR":
            print("\nReceived a COORDINATOR message")
            self.node.handle_coordinator_message(data[1])

        elif data[0] == "PROBE":
            print("\nReceived a PROBE message. Returning an OK...")
            self.request.send(pickle.dumps("OK"))

        else:
            print("\nReceived an unexpected message. Ignoring message.")


if __name__ == "__main__":
    """The entry point for the distributed system. Parses command-line arguments,
    initializes a Node instance, and starts the system with the provided parameters.

    Usage: python lab2.py GCD_HOSTNAME GCD_PORT YOUR_SU_ID YOUR_MOTHERS_BDAY

    Args:
        sys.argv (list): Command-line arguments provided when running the script.

    Raises:
        SystemExit: Exits the program if the correct number of arguments is not provided.
    """
    if len(sys.argv) != 5:
        print("Usage: python lab2.py GCD_HOSTNAME GCD_PORT YOUR_SU_ID YOUR_MOTHERS_BDAY")
        exit(1)
    # Arguments 1 & 2 correspond to the GCD host and port via the console
    # Arguments 3 & 4 correspond to your SeattleU ID number and your mother's bday in YYYY-MM-DD format
    node = Node((sys.argv[3], sys.argv[4]), (sys.argv[1], int(sys.argv[2])))
