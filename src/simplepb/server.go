package simplepb

//
// This is a outline of primary-backup replication 
// 	based on a simplifed version of Viewstamp replication.
//
//
//

import (
	"sync"

	"labrpc"

	// "fmt"
)

// the 3 possible server status
const (
	NORMAL = iota // iota = 0
	VIEWCHANGE // iota = 1
	RECOVERING // iota = 2
)

// PBServer defines the state of a replica server (either primary or backup)
type PBServer struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	me             int                 // this peer's index into peers[]
	currentView    int                 // what this peer believes to be the current active view
	status         int                 // the server's current status (NORMAL, VIEWCHANGE or RECOVERING)
	lastNormalView int                 // the latest view which had a NORMAL status

	log         []interface{} // the log of "commands"
	commitIndex int           // all log entries <= commitIndex are considered to have been committed.

	// ... other state that you might need ...
	prepareRetry   int // number of prepare retries if no enough success recieved
}

// Prepare defines the arguments for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC args struct
type PrepareArgs struct {
	View          int         // the primary's current view
	PrimaryCommit int         // the primary's commitIndex
	Index         int         // the index position at which the log entry is to be replicated on backups
	Entry         interface{} // the log entry to be replicated
}

// PrepareReply defines the reply for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC reply struct
type PrepareReply struct {
	View    int  // the backup's current view
	Success bool // whether the Prepare request has been accepted or rejected
}

// RecoverArgs defined the arguments for the Recovery RPC
type RecoveryArgs struct {
	View   int // the view that the backup would like to synchronize with
	Server int // the server sending the Recovery RPC (for debugging)
}

type RecoveryReply struct {
	View          int           // the view of the primary
	// the primary's log including entries replicated up to and including the view.
	Entries       []interface{} 
	PrimaryCommit int           // the primary's commitIndex
	Success       bool          // whether the Recovery request has been accepted or rejected
}

type ViewChangeArgs struct {
	View int // the new view to be changed into
}

type ViewChangeReply struct {
	LastNormalView int           // the latest view which had a NORMAL status at the server
	Log            []interface{} // the log at the server
	Success        bool          // whether the ViewChange request has been accepted/rejected
}

type StartViewArgs struct {
	View int           // the new view which has completed view-change
	Log  []interface{} // the log associated with the new new
}

type StartViewReply struct {
}

// GetPrimary is an auxilary function that returns the server index of the
// primary server given the view number (and the total number of replica servers)
func GetPrimary(view int, nservers int) int {
	return view % nservers
}

// IsCommitted is called by tester to check whether an index position
// has been considered committed by this server
func (srv *PBServer) IsCommitted(index int) (committed bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.commitIndex >= index {
		return true
	}
	return false
}

// ViewStatus is called by tester to find out the current view of this server
// and whether this view has a status of NORMAL.
func (srv *PBServer) ViewStatus() (currentView int, statusIsNormal bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.currentView, srv.status == NORMAL
}

// GetEntryAtIndex is called by tester to return the command replicated at
// a specific log index. If the server's log is shorter than "index", then
// ok = false, otherwise, ok = true
func (srv *PBServer) GetEntryAtIndex(index int) (ok bool, command interface{}) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.log) > index {
		return true, srv.log[index]
	}
	return false, command
}

// Kill is called by tester to clean up (e.g. stop the current server)
// before moving on to the next test
func (srv *PBServer) Kill() {
	// Your code here, if necessary
}

// Make is called by tester to create and initalize a PBServer
// peers is the list of RPC endpoints to every server (including self)
// me is this server's index into peers.
// startingView is the initial view (set to be zero) that all servers start in
func Make(peers []*labrpc.ClientEnd, me int, startingView int) *PBServer {
	srv := &PBServer{
		peers:          peers,
		me:             me,
		currentView:    startingView,
		lastNormalView: startingView,
		status:         NORMAL,
	}
	// all servers' log are initialized with a dummy command at index 0
	var v interface{}
	srv.log = append(srv.log, v)

	// Your other initialization code here, if there's any
	srv.commitIndex = 0 // should already be initialized 
	// prepareChan := make(chan int)
	return srv
}

// Start() is invoked by tester on some replica server to replicate a
// command.  Only the primary should process this request by appending
// the command to its log and then return *immediately* 
//  (while the log is being replicated to backup servers).
// if this server isn't the primary, returns false.
// Note that since the function returns immediately, there is no guarantee that this command
// will ever be committed upon return, since the primary
// may subsequently fail before replicating the command to all servers
//
// The first return value is the index that the command will appear at
// *if it's eventually committed*. The second return value is the current
// view. The third return value is true if this server believes it is
// the primary.
func (srv *PBServer) Start(command interface{}) (index int, view int, ok bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	// do not process command if status is not NORMAL
	// and if i am not the primary in the current view
	if srv.status != NORMAL {
		return -1, srv.currentView, false
	} else if GetPrimary(srv.currentView, len(srv.peers)) != srv.me {
		return -1, srv.currentView, false
	}

	// Your code here
	// 	When Start(command) is invoked, 
	// 	the primary should append the command in its log
	// fmt.Printf("Primary: %d, view: %d, status: %d, index: %d, commitIndex: %d\n", 
	// 	srv.me, srv.currentView, srv.status, len(srv.log), srv.commitIndex)
	srv.lastNormalView = srv.currentView

	srv.log = append(srv.log, command)
	// 	and then send Prepare RPCs to other servers 
	// to instruct them to replicate the command in the same index in their log.
	args := &PrepareArgs {
		View : srv.currentView,
		PrimaryCommit : srv.commitIndex,
		Index : len(srv.log) - 1,
		Entry : command }
	go srv.sendPrepareToPeers(args)

	return len(srv.log) - 1, srv.currentView, true
	// return index, view, ok
}

// func (srv *PBServer) increasePrimaryCommitIndex(confirmCommitChan chan int){
// 	select {
// 	case <- confirmCommitChan:
// 		srv.mu.Lock()
// 		defer srv.mu.Unlock()
// 		srv.commitIndex ++
// 		fmt.Printf("Commit: primary: %d, view: %d, status: %d, index: %d, commitIndex: %d\n", 
// 			srv.me, srv.currentView, srv.status, len(srv.log), srv.commitIndex)
// 	}
// }

// If the primary has received Success=true responses from a majority of servers (including 
// itself), it considers the corresponding log index as "committed". (Since servers process 
// Prepare messages), It advances thecommittedIndex field locally and also piggybacks this 
// information to backup servers in subsequent Prepares.
func (srv *PBServer) waitPrepareToCommit(
	args *PrepareArgs, prepareReplyChan chan *PrepareReply, majorityResponseNumber int){

	successResponseNumber := 0
	totalResponseNumber := 0
	for i := 0; i < len(srv.peers); i++ {
		reply := <- prepareReplyChan
		totalResponseNumber ++
		if reply != nil && reply.Success == true {
			successResponseNumber ++
		}
		if( successResponseNumber >= majorityResponseNumber ){
			// wait till previous prepare is committed
			for {
				srv.mu.Lock()
				if srv.commitIndex + 1 == args.Index {
					srv.commitIndex = args.Index
					// fmt.Printf("Commit: primary: %d, view: %d, status: %d, index: %d, commitIndex: %d\n", 
					// 	srv.me, srv.currentView, srv.status, len(srv.log), srv.commitIndex)
					srv.mu.Unlock()
					break
				}
				srv.mu.Unlock()
			}
		}
	}

	// Not enough success prepare response recieved, resend prepare
	go srv.sendPrepareToPeers(args)

}

func (srv *PBServer) sendPrepareToPeers(args *PrepareArgs) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	prepareReplyChan := make(chan *PrepareReply, len(srv.peers))
	
	for i := 0; i < len(srv.peers); i++ {
		go func(server int) {
			var reply PrepareReply
			ok := srv.sendPrepare(server, args, &reply)
			if ok {
				prepareReplyChan <- &reply
				// successResponseNumber += 1 // might cause problems if without sync ????????????
				// fmt.Printf("Recieved server %d success\n", server)
			} else {
				prepareReplyChan <- nil
				// fmt.Printf("Recieved server %d failed\n", server)
			}
		}(i)
	}

	majorityResponseNumber := len(srv.peers)/2 + 1	
	go srv.waitPrepareToCommit(args, prepareReplyChan, majorityResponseNumber)
	
}


// exmple code to send an AppendEntries RPC to a server.
// server is the index of the target server in srv.peers[].
// expects RPC arguments in args.
// The RPC library fills in *reply with RPC reply, so caller should pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
func (srv *PBServer) sendPrepare(server int, args *PrepareArgs, reply *PrepareReply) bool {
	ok := srv.peers[server].Call("PBServer.Prepare", args, reply)
	return ok
}

// func (srv *PBServer) processPrepareMsg()

// Prepare is the RPC handler for the Prepare RPC
func (srv *PBServer) Prepare(args *PrepareArgs, reply *PrepareReply) {
	// Your code here
	// Upon receiving a Prepare RPC message, the backup checks whether the message's view 
	// and its currentView match and whether the next entry to be added to the log is indeed 
	// at the index specified in the message. If so, the backup adds the message's entry to the log 
	// and replies Success=ok. Otherwise, the backup replies Success=false. 
	
	// Furthermore, if the backup's state falls behind the primary(e.g. its view is smaller 
	// or its log is missing entries), it performs recovery to transfer the primary's log. 
	// Note that the backup server needs to process Prepare messages according to their index order, 
	// otherwise, it would end up unnecessarily rejecting many messages.
	srv.mu.Lock()
	// defer srv.mu.Unlock()	

	// fmt.Printf("Prepare: node: %d, view: %d, status: %d, index: %d, commitIndex: %d\n", 
	// 	srv.me, srv.currentView, srv.status, len(srv.log), srv.commitIndex)
	// fmt.Printf("Recieved: pmView: %d, pmCommitIndex: %d, logIndex: %d\n", 
	// 	args.View, args.PrimaryCommit, args.Index)

	primaryView := args.View


	if( srv.currentView == primaryView && len(srv.log) == args.Index ){
		srv.prepareRetry = 0
		srv.lastNormalView = srv.currentView

		srv.log = append(srv.log, args.Entry)
		reply.View, reply.Success = srv.currentView, true
		srv.mu.Unlock()
		return 
	}

	if( srv.currentView == primaryView && len(srv.log) > args.Index ){
		// already processed this prepare, reconfirm without logging
		// or srv is the primary
		reply.View, reply.Success = srv.currentView, true
		srv.mu.Unlock()
		return 
	}

	if( srv.currentView > primaryView ){
		reply.View, reply.Success = srv.currentView, false
		srv.mu.Unlock()
		return 
	}

	if( srv.prepareRetry >= 100 ){

		//start recovering
		srv.prepareRetry = 0
		srv.status = RECOVERING

		go func(view int) {
			recoveryArgs := &RecoveryArgs { View: primaryView, Server: srv.me }
			var recoveryReply RecoveryReply

			ok := srv.sendRecovery( 
				GetPrimary(view, len(srv.peers)), recoveryArgs, &recoveryReply)

			if( ok == true && recoveryReply.Success == true ){
				//recover the server's state
				srv.mu.Lock()
				srv.currentView = recoveryReply.View
				srv.lastNormalView = srv.currentView
				srv.log = recoveryReply.Entries
				srv.commitIndex = recoveryReply.PrimaryCommit
				srv.status = NORMAL
				srv.mu.Unlock()
			}

		}(primaryView)

		reply.View, reply.Success = srv.currentView, false
		srv.mu.Unlock()
		return 
	} else {
		srv.prepareRetry ++
		srv.mu.Unlock()
		srv.sendPrepare(srv.me, args, reply)
	}

	// if( srv.commitIndex < primaryCommit ){
	// 	// update backup srv.commitIndex
	// 	srv.commitIndex = primaryCommit // note here len(srv.log) >= primaryCommit >= srv.commitIndex
	// }

	// if( srv.currentView == primaryView ){

	// 	// wait for the ( len(srv.log) + 1 )
	// 	for len(srv.log) < logIndex {
	// 		// len(srv.log) - 1 + 1 == logIndex 
	// 	}

	// 	// Process prepare
	// 	if ( GetPrimary(srv.currentView, len(srv.peers)) == srv.me ) {
	// 		// Primary recieve Prepare, return directly
	// 		reply.View = srv.currentView
	// 		reply.Success = true
	// 		return
	// 	} else { // Back up nodes, 
	// 		srv.log = append(srv.log, logEntry)
	// 		// srv.commitIndex ++ 		// should wait for the next prepare to update commitIndex
	// 		reply.View = srv.currentView
	// 		reply.Success = true
	// 		return 
	// 	}
	// } else{
	// 	fmt.Printf("Error in Prepare(), backup view > primaryView\n")
	// }
}

func (srv *PBServer) sendRecovery(server int, args *RecoveryArgs, reply *RecoveryReply) bool {
	ok := srv.peers[server].Call("PBServer.Recovery", args, reply)
	return ok
}

// Recovery is the RPC handler for the Recovery RPC
func (srv *PBServer) Recovery(args *RecoveryArgs, reply *RecoveryReply) {
	// Your code here
	reply.View, reply.Entries, reply.PrimaryCommit = 
		srv.currentView, srv.log, srv.commitIndex
	if( srv.status == NORMAL ) {
		 reply.Success = true
	} else {
		reply.Success = false
	}
	return
}

// Some external oracle prompts the primary of the newView to switch to the newView.
// PromptViewChange just kicks start the view change protocol to move to the newView
// It does not block waiting for the view change process to complete.
func (srv *PBServer) PromptViewChange(newView int) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	newPrimary := GetPrimary(newView, len(srv.peers))

	if newPrimary != srv.me { //only primary of newView should do view change
		return
	} else if newView <= srv.currentView {
		return
	}
	vcArgs := &ViewChangeArgs{
		View: newView,
	}
	vcReplyChan := make(chan *ViewChangeReply, len(srv.peers))
	// send ViewChange to all servers including myself
	for i := 0; i < len(srv.peers); i++ {
		go func(server int) {
			var reply ViewChangeReply
			ok := srv.peers[server].Call("PBServer.ViewChange", vcArgs, &reply)
// fmt.Printf("node-%d (nReplies %d) received reply ok=%v reply=%v\n", srv.me, nReplies, ok, r.reply)
			if ok {
				vcReplyChan <- &reply
			} else {
				vcReplyChan <- nil
			}
		}(i)
	}

	// wait to receive ViewChange replies
	// if view change succeeds, send StartView RPC
	go func() {
		var successReplies []*ViewChangeReply
		var nReplies int
		majority := len(srv.peers)/2 + 1
		for r := range vcReplyChan {
			nReplies++
			if r != nil && r.Success {
				successReplies = append(successReplies, r)
			}
			if nReplies == len(srv.peers) || len(successReplies) == majority {
				break
			}
		}
		ok, log := srv.determineNewViewLog(successReplies)
		if !ok {
			return
		}
		svArgs := &StartViewArgs{
			View: vcArgs.View,
			Log:  log,
		}
		// send StartView to all servers including myself
		for i := 0; i < len(srv.peers); i++ {
			var reply StartViewReply
			go func(server int) {
// fmt.Printf("node-%d sending StartView v=%d to node-%d\n", srv.me, svArgs.View, server)
				srv.peers[server].Call("PBServer.StartView", svArgs, &reply)
			}(i)
		}
	}()
}

// determineNewViewLog is invoked to determine the log for the newView based on
// the collection of replies for successful ViewChange requests.
// if a quorum of successful replies exist, then ok is set to true.
// otherwise, ok = false.

// If the primary has received successful ViewChange replies from a majority of servers 
// (including itself). It can proceed to start the new view. It needs to start the new-view 
// with a log that contains all the committed entries of the previous view. To maintain 
// this invariant, the primary chooses the log among the majority of successful replies 
// using this rule: it picks the log whose lastest normal view number is the largest. If 
// there are more than one such logs, it picks the longest log among those. 
func (srv *PBServer) determineNewViewLog(successReplies []*ViewChangeReply) (
	ok bool, newViewLog []interface{}) {
	// Your code here
	newMaxView := -1
	var newMaxLog []interface{}

	if( len(successReplies) < len(srv.peers)/2 + 1 ){
		return false, newMaxLog
	}

	ok = false
	for _ , r := range successReplies {
		if( r.LastNormalView > newMaxView ){
			newMaxView = r.LastNormalView
			newMaxLog = r.Log
			ok = true
		} else if( r.LastNormalView == newMaxView ){
			if( len(r.Log) > len(newMaxLog) ){
				newMaxLog = r.Log
			}
		} else{
			continue
		}
	}
	return ok, newMaxLog
}

// ViewChange is the RPC handler to process ViewChange RPC.
 // Upon receving ViewChange, a replica server checks that the view number included in 
 // the message is indeed larger than what it thinks the current view number is. If 
 // the check succeeds, it sets its current view number to that in the message and modifies 
 // its status to VIEWCHANGE. It replies Success=true and includes its current log (in 
 // its entirety) as well as the latest view-number that has been considered NORMAL. If the 
 // check fails, the backup replies Success=false.
func (srv *PBServer) ViewChange(args *ViewChangeArgs, reply *ViewChangeReply) {
	// Your code here
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if( srv.currentView < args.View && srv.status == NORMAL ){
		srv.lastNormalView = srv.currentView
		// srv.currentView = args.View
		srv.status = VIEWCHANGE

		reply.LastNormalView, reply.Log, reply.Success = srv.lastNormalView, srv.log, true
	} else{
		reply.LastNormalView, reply.Log, reply.Success = srv.lastNormalView, srv.log, false
	}
}

// StartView is the RPC handler to process StartView RPC.

// Once the primary has determined the log for the new-view, it sends out the StartView RPC 
// to all servers to instruct them to start the new view. Upon receive StartView, a server 
// sets the new-view as indicated in the message and changes its status to be NORMAL. Note 
// that before setting the new-view according to the StartView RPC message, the server must 
// again check that its current view is no bigger than that in the RPC message, which would 
// mean that there's been no concurrent view-change for a larger view.
func (srv *PBServer) StartView(args *StartViewArgs, reply *StartViewReply) {
	// Your code here
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if( srv.currentView <= args.View ){
		srv.currentView = args.View
		srv.status = NORMAL
		srv.lastNormalView = srv.currentView
		srv.log = args.Log
		srv.commitIndex = len(srv.log) - 1
	}
}
