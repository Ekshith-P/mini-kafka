package com.minikafka.raft;

/** The three roles a Raft server can be in at any moment. */
public enum RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}