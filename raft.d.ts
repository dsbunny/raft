// Type definitions for [~THE LIBRARY NAME~] [~OPTIONAL VERSION NUMBER~]
// Project: [~THE PROJECT NAME~]
// Definitions by: [~YOUR NAME~] <[~A URL FOR YOU~]>
export class Raft extends EventTarget {
        static LEADER: number;
        static CANDIDATE: number;
        static FOLLOWER: number;
        static states: any;
	static RPC_REQUEST_VOTE: string;
	static RPC_VOTE: string;
	static RPC_APPEND_ENTRY: string;
	constructor(settings: any);
	get leader(): string;
	onRaftMessage(data: string, sendReply: (data: string) => void): void;
	update(timestamp: number): void;
	join(address: string, send: (data: string) => void): void;
	leave(address: string): void;
	close(): void;
}
