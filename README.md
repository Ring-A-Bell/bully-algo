# Lab 2: Leader Election in Distributed Systems via Bully Algorithm

This project implements a node in a fully interconnected group using the Bully algorithm for leader election. The purpose is to demonstrate understanding of distributed systems concepts and algorithms, particularly consensus and leader election.

## Overview

In this lab, we expand on Lab 1 by joining a group that is fully interconnected (like a round table) and selecting a leader using the Bully algorithm. Choosing a leader is a form of consensus in distributed systems.

## Features

- Each node in the group has an identity, represented as the pair: (days until your mother's next birthday, your SU ID).
- Nodes join the group by contacting the Group Coordinator Daemon (GCD).
- Nodes participate in elections.
- Nodes detect when the current leader has failed and initiate a new election.
- Nodes can feign failure and subsequently recover for extra credit.

## Implementation Details

- The project is implemented in Python 3.
- Communication between nodes is done via TCP sockets.
- All messages are pickled and have the format (message_name, message_data).
- Messages include `JOIN`, `ELECTION`, `COORDINATOR`, and `PROBE`.

## Protocol

#### JOIN Message

- When starting up, a node contacts the GCD and sends a `JOIN` message, containing its process ID and listening address.
- The response from the GCD is a list of all other members in the group.

#### ELECTION Message

- Nodes initiate an election when they join the group or notice the leader has failed.
- The `ELECTION` message is sent to each member with a higher process ID.
- The node winning the election sends a `COORDINATOR` message to everyone.

#### COORDINATOR Message

- Sent by the group leader when it wins an election.
- Contains the new leader's identity.

#### PROBE Message (Extra Credit)

- Sent occasionally to the group leader by all other members.
- Triggers a new election if failed.

#### Feigning Failure (Extra Credit)

- Nodes can pretend to fail and recover at random intervals.

## Getting Started

1. Clone the repository.
2. Run the Python scripts for the lab exercises.
3. Follow the instructions provided in the docstrings to run/start the program.

## Additional Notes

- Ensure non-blocking communication using ThreadingTCPServer.
- Be cautious of race conditions when accessing shared data structures.
- Reference Python.org's socket how-to for guidance.
- Utilize the provided `gcd2.py` for testing purposes.


## Credits

- Example threaded server based on python.org's socketserver documentation page by Kevin Lundeen for *SeattleU/CPSC5520*
