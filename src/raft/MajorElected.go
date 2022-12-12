package raft

import "sync"

type MajorElected struct {
	RaftStateTransfer
}

func (trans *MajorElected) getName() string {
	return "MajorElected"
}

func (trans *MajorElected) isRW() bool {
	return false
}

func (rf *Raft) makeMajorElected() *MajorElected {
	return &MajorElected{RaftStateTransfer{machine: rf.stateMachine}}
}

func (trans *MajorElected) transfer(source SMState) SMState {
	// check state
	if source != startElectionState && source != sendAEState {
		return notTransferred
	}
	if source == startElectionState {
		// init volatile
		trans.machine.raft.Infof("first sendAE after elected")
	}
	trans.machine.raft.electionTimer.stop()
	// start send AE
	go trans.machine.raft.sendAEs()
	// set timer
	trans.machine.raft.sendAETimer.start()

	trans.machine.raft.persist()

	return sendAEState
}

func (rf *Raft) sendSingleAE(server int, joinCount *int, cond *sync.Cond) {
	rf.stateMachine.rwmu.RLock()
	args := AppendEntriesArgs{
		Term:     rf.stateMachine.currentTerm,
		LeaderId: rf.me,
	}
	rf.Infof("sending to server %d", server)
	rf.stateMachine.rwmu.RUnlock()
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, &args, &reply)

	if ok {
		rf.stateMachine.rwmu.RLock()
		rf.Infof("reply from follower %d success %t", server, reply.Success)
		if reply.Term > rf.stateMachine.currentTerm {
			rf.stateMachine.issueTransfer(rf.makeLargerTerm(reply.Term, server))
		}
		rf.stateMachine.rwmu.RUnlock()
	}
	cond.L.Lock()
	*joinCount++
	if *joinCount+1 >= rf.peerCount() {
		cond.Broadcast()
	}
	cond.L.Unlock()
}

func (rf *Raft) sendAEs() {
	joinCount := 0
	cond := sync.NewCond(&sync.Mutex{})
	rf.stateMachine.rwmu.RLock()

	rf.stateMachine.rwmu.RUnlock()
	for i := 0; i < rf.peerCount(); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendSingleAE(i, &joinCount, cond)
	}
	cond.L.Lock()
	for joinCount+1 < rf.peerCount() {
		cond.Wait()
	}
	cond.L.Unlock()
}
