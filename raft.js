// vim: tabstop=8 softtabstop=0 noexpandtab shiftwidth=8 nosmarttab
// REF: https://raft.github.io/raft.pdf

import * as msgpack from '@msgpack/msgpack';

// Encapsulation of a node address and a function to send a message to it.
class RaftNode {
	#address;
	#send;
	constructor(address, send) {
		this.#address = address;
		this.#send = send;
	}
	get address() { return this.#address; }
	send(data) { this.#send(data); }
}

// Encapsulation of a message used in the Raft protocol.  This implementation
// provides two serialization options: JSON object, and JSON array.
class RaftMessage {
	#state;
	#leader;
	#term;
	#address;
	#type;
	#granted;
	constructor(state, leader, term, address, type, granted) {
		this.#state = state;
		this.#leader = leader;
		this.#term = term;
		this.#address = address;
		this.#type = type;
		this.#granted = granted;
	}
	get state() { return this.#state; }
	get leader() { return this.#leader; }
	get term() { return this.#term; }
	get address() { return this.#address; }
	get type() { return this.#type; }
	get granted() { return this.#granted; }
	toJSON() {
		return {
			state: this.#state,
			leader: this.#leader,
			term: this.#term,
			address: this.#address,
			type: this.#type,
			granted: this.#granted,
		};
	}
	static fromJSON(json) {
		const msg = new RaftMessage(json.state, json.leader, json.term, json.address, json.type, json.granted);
		return msg;
	}
	toArray() {
		return [
			this.#state,
			this.#leader,
			this.#term,
			this.#address,
			this.#type,
			this.#granted,
		]
	}
	static from(array) {
		return new RaftMessage(array[0], array[1], array[2], array[3], array[4], array[5]);
	}
	toBuffer() {
		return msgpack.encode(this.toArray());
	}
	static fromBuffer(buffer) {
		const array = msgpack.decode(buffer);
		return RaftMessage.from(array);
	}
}

export class Raft extends EventTarget {
	// §5.1: At any given time, in one of three states: leader, follower,
	// or candidate.
	static LEADER = 1;
	static CANDIDATE = 2;
	static FOLLOWER = 3;
	static states = {
		1: "LEADER",
		2: "CANDIDATE",
		3: "FOLLOWER",
	};

	static RPC_REQUEST_VOTE = "poll";  // Initiated by candidates during elections.
	static RPC_VOTE = "vote";
	static RPC_APPEND_ENTRY = "append";  // Initiated by leaders as heartbeat.

	#address;
	#electionMinTimeout;
	#electionMaxTimeout;
	#heatbeatInterval;

	#heartbeatExpiration = undefined;
	#electionExpiration = undefined;

	#votesFor = undefined;
	#votesGranted = 0;

	#nodes = [];

	// §5.2: When nodes start up, they begin as followers.
	#state = Raft.FOLLOWER;
	#leader = undefined;
	#currentTerm = 0;

	constructor(settings) {
		super();
		this.#address = settings.address;
		this.#electionMinTimeout = settings.electionMinTimeout;
		this.#electionMaxTimeout = settings.electionMaxTimeout;
		this.#heatbeatInterval = settings.heartbeatInterval;

		// §5.2: If a follower receives no communication over a period of time
		// called the election timeout, then it assumes there is no viable
		// leader and begins an election to choose a new leader.
		this.#electionExpiration = performance.now() + this.#electionTimeout();
	}

	get leader() { return this.#state === Raft.LEADER; }

	// §5.2: A candidate wins an election if it receives votes from a
	// majority of the servers in the full cluster for the same term.
	#resetVotes() {
		this.#votesFor = undefined;
		this.#votesGranted = 0;
	}

	onRaftMessage(data, sendReply) {
//		console.log(`RAFT: onRaftMessage(${data})`);
		const msg = RaftMessage.fromBuffer(data);
		// §5.1:  Current terms are exchanged whenever servers
		// communicate
		if(msg.term > this.#currentTerm) {
			// §5.1: If a candidate or leader discovers that its
			// term is out of date, it immediately reverts to
			// follower state.
			if(this.#state !== Raft.FOLLOWER) {
				this.#state = Raft.FOLLOWER;
			}
			this.#heartbeatExpiration = undefined;
			this.#electionExpiration = performance.now() + this.#electionTimeout();
			if(this.#leader !== msg.leader) {
				this.#leader = msg.leader;
			}
			this.#currentTerm = msg.term;
			this.#resetVotes();
		} else if(msg.term < this.#currentTerm) {
			// §5.1: If a server receives a request with a stale
			// term number, it rejects the request
			return;
		}

		switch(this.#state) {
		case Raft.FOLLOWER:
			switch(msg.type) {
			case Raft.RPC_REQUEST_VOTE:
				this.#onRequestVote(msg, sendReply);
				break;
			case Raft.RPC_APPEND_ENTRY:
				// §5.2: Leaders send periodic heartbeats to
				// all followers in order to maintain their
				// authority.
				this.#electionExpiration = performance.now() + this.#electionTimeout();
				break;
			default:
				break;
			}
			break;
		case Raft.CANDIDATE:
			switch(msg.type) {
			case Raft.RPC_VOTE:
				this.#onVote(msg);
				break;
			case Raft.RPC_APPEND_ENTRY:
				// §5.2: If the leader’s term is at least
				// as large as the candidate’s current term,
				// then the candidate recognizes the leader as
				// legitimate and returns to follower state.
				this.#leader = msg.leader;
				this.#state = Raft.FOLLOWER;
				this.#heartbeatExpiration = undefined;
				this.#electionExpiration = performance.now() + this.#electionTimeout();
				break;
			default:
				break;
			}
			break;
		default:
			break;
		}
	}

	// §5.2: Each node will vote for at most one candidate in a
	// given term, on a first-come-first-served basis.
	#onRequestVote(msg, sendReply) {
		if(typeof this.#votesFor !== "undefined"
			&& msg.address !== this.#votesFor)
		{
			const msg = new RaftMessage(this.#state, this.#leader, this.#currentTerm, this.#address, Raft.RPC_VOTE, false);
			sendReply(msg.toBuffer());
			return;
		}

		this.#leader = msg.address;
		if(this.#currentTerm !== msg.term) {
			this.#currentTerm = msg.term;
			this.#resetVotes();
		}
		const reply = new RaftMessage(this.#state, this.#leader, this.#currentTerm, this.#address, Raft.RPC_VOTE, true);
		sendReply(reply.toBuffer());
		this.#electionExpiration = performance.now() + this.#electionTimeout();
	}

	#onVote(msg) {
		if(this.#state !== Raft.CANDIDATE) {
			return;
		}
		if(msg.granted) {
			this.#votesGranted++;
		}
		// §5.2: A candidate wins an election if it receives votes from
		// a majority of the servers in the full cluster for the same
		// term.
		if(this.#hasQuorum(this.#votesGranted)) {
			// §5.2: Once a candidate wins an election, it becomes
			// leader.
			this.#becomeLeader();
		}
	}

	#becomeLeader() {
		this.#state = Raft.LEADER;
		this.#leader = this.#address;
		this.#heartbeatExpiration = performance.now() + this.#heatbeatInterval;
		this.#electionExpiration = undefined;
		// §5.2: Send heartbeat messages to all of the other servers to
		// establish its authority and prevent new elections.
		const reply = new RaftMessage(this.#state, this.#leader, this.#currentTerm, this.#address, Raft.RPC_APPEND_ENTRY);
		this.#sendRaftMessage(Raft.FOLLOWER, reply);
	}

	#hasQuorum(vote_count) {
		return vote_count >= this.#majorityCount();
	}

	#majorityCount() {
		return Math.ceil(this.#nodes.length / 2) + 1;
	}

	update(timestamp) {
		if(timestamp >= this.#heartbeatExpiration) {
			this.#onHeartbeatExpiration();
		}
		if(timestamp >= this.#electionExpiration) {
			this.#onElectionExpiration();
		}
	}

	// §5.2: Leaders send periodic heartbeats to all followers in
	// order to maintain their authority.
	#onHeartbeatExpiration() {
		if(this.#state !== Raft.LEADER) {
			this.#heartbeatExpiration = undefined;
			return;
		}
		const msg = new RaftMessage(this.#state, this.#leader, this.#currentTerm, this.#address, Raft.RPC_APPEND_ENTRY);
		this.#sendRaftMessage(Raft.FOLLOWER, msg);
		this.#heartbeatExpiration = performance.now() + this.#heatbeatInterval;
	}

	// §5.2: If a follower receives no communication over a period of time
	// called the election timeout, then it assumes there is no viable
	// leader and begins an election to choose a new leader.
	#onElectionExpiration() {
		if(this.#state === Raft.LEADER) {
			this.#electionExpiration = undefined;
			return;
		}
		this.#startElection();
	}

	#sendRaftMessage(target, msg) {
//		console.log(`#sendRaftMessage(${Raft.states[target]}, ${msg.toString()})`);
		const data = msg.toBuffer();
		switch(target) {
		case Raft.FOLLOWER:
			for(const node of this.#nodes) {
				if(node.address === this.#leader) {
					continue;
				}
				node.send(data);
			}
			break;
		default:
			break;
		}
	}

	// §5.2: Raft uses randomized election timeouts to ensure that
	// split votes are rare and that they are resolved quickly.
	#electionTimeout() {
		return Math.random() * (this.#electionMaxTimeout - this.#electionMinTimeout + 1) + this.#electionMinTimeout;
	}

	// §5.2: To begin an election, a follower increments its current
	// term and transitions to candidate state.
	#startElection() {
//		console.log("#startElection");
		if(this.#state !== Raft.CANDIDATE) {
			this.#state = Raft.CANDIDATE;
		}
		this.#heartbeatExpiration = undefined;
		this.#electionExpiration = performance.now() + this.#electionTimeout();
		this.#leader = undefined;
		this.#currentTerm++;

		// §5.2: Vote for self.
		this.#votesFor = this.#address;
		this.#votesGranted = 1;

		// §5.2: Issues RequestVote RPCs in parallel to each of
		// the other nodes in the cluster.
		const msg = new RaftMessage(this.#state, this.#leader, this.#currentTerm, this.#address, Raft.RPC_REQUEST_VOTE);
		this.#sendRaftMessage(Raft.FOLLOWER, msg);
	}

	join(address, send) {
		console.log(`RAFT: join(${address})`);
		if(address === this.#address) {
			return;
		}
		const pos = this.#nodes.findIndex(node => node.address === address);
		if(pos !== -1) {
			return;
		}
		this.#nodes.push(new RaftNode(address, send));
		this.dispatchEvent(new CustomEvent('join', {
			detail: {
				address,
			}
		}));
	}

	leave(address) {
		console.log(`RAFT: leave(${address})`);
		const pos = this.#nodes.findIndex(node => node.address === address);
		if(pos === -1) {
			return;
		}
		this.#nodes.splice(pos, 1);
		this.dispatchEvent(new CustomEvent('leave', {
			detail: {
				address,
			}
		}));
	}

	close() {
		console.log(`RAFT: close)`);
		if(this.#state === 0) {
			return;
		}
		this.#state = 0;
		this.dispatchEvent(new CustomEvent('closing'));
		for(const node of this.#nodes) {
			this.leave(node.address);
		}
		this.dispatchEvent(new CustomEvent('closed'));
		this.#heartbeatExpiration = undefined;
		this.#electionExpiration = undefined;
	}
}
