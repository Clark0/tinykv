// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// logIndex to slice index
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	return &RaftLog{
		storage:   storage,
		entries:   entries,
		stabled:   lastIndex,
		committed: hardState.Commit,
		applied:   firstIndex - 1,
		offset:    firstIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.stabled+1-l.offset:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		lo, hi := l.applied+1-l.offset, l.committed+1-l.offset
		return l.entries[lo:hi]
	}
	return nil
}

func (l *RaftLog) Slice(loIndex uint64, hiIndex uint64) []*pb.Entry {
	lo, hi := loIndex-l.offset, hiIndex-l.offset
	if lo >= 0 && hi <= uint64(len(l.entries)) {
		ent := make([]*pb.Entry, hi - lo)
		for i := lo; i < hi; i++ {
			ent[i-lo] = &l.entries[i]
		}
		return ent
	}
	return nil
}

// Truncate entries since loIndex
func (l *RaftLog) TruncateEntriesFrom(loIndex uint64) {
	l.stabled = min(l.stabled, loIndex-1)
	lo := loIndex - l.offset
	if lo >= 0 && lo < uint64(len(l.entries)) {
		l.entries = l.entries[:lo]
	}
}

func (l *RaftLog) appendEntries(ents []*pb.Entry) {
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if size := len(l.entries); size != 0 {
		return l.offset + uint64(size) - 1
	}
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	if len(l.entries) > 0 && i >= l.offset {
		return l.entries[i-l.offset].Term, nil
	}
	term, err := l.storage.Term(i)
	return term, err
}
