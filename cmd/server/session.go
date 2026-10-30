package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/signaling"
	"wacalls/internal/voip/wanode"
	"wacalls/internal/wa"

	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type Session struct {
	id   string
	name string
	mgr  *SessionManager
	log  *slog.Logger

	client *whatsmeow.Client
	reg    *callRegistry

	mu   sync.Mutex
	auth AuthSnapshot
}

func newSession(mgr *SessionManager, id, name string, client *whatsmeow.Client) *Session {
	s := &Session{
		id:     id,
		name:   name,
		mgr:    mgr,
		log:    mgr.log.With("session", id),
		client: client,
		auth:   AuthSnapshot{State: "connecting"},
		reg:    newCallRegistry(),
	}
	client.AddEventHandler(s.handleEvent)
	return s
}

func (s *Session) createCall(callID string) *call.CallManager {
	cm := call.NewCallManager(wa.NewSocket(s.client), s.log)
	s.wireCall(cm, callID)
	s.reg.add(callID, &activeCall{cm: cm})
	return cm
}

func (s *Session) wireCall(cm *call.CallManager, callID string) {
	cm.OnIncoming = func(c *call.CallInfo) {
		s.mgr.broker.upsertCall(CallRecord{
			SessionID: s.id, CallID: c.CallID, Direction: "inbound", Peer: c.PeerJid,
			StartedAt: time.Now().UnixMilli(), Status: StatusRinging,
		})
		s.mgr.broker.emitIncoming(s.id, c.CallID, c.PeerJid)
	}
	cm.OnStateChange = func(c *call.CallInfo) {
		s.log.Info("[DEBUG] OnStateChange", "call_id", c.CallID, "state", string(c.StateData.State), "direction", string(c.Direction), "ended", c.IsEnded())

		if c.IsEnded() {
			if ac, ok := s.reg.get(c.CallID); ok && ac.callTimeout != nil {
				ac.callTimeout.Stop()
			}
			s.removeCall(c.CallID)
			s.mgr.broker.endCall(c.CallID, string(c.StateData.EndReason))
			return
		}
		dir := "outbound"
		if c.Direction == core.CallDirectionIncoming {
			dir = "inbound"
		}
		existing, _ := s.mgr.broker.getCall(c.CallID)
		rec := CallRecord{
			SessionID: s.id, CallID: c.CallID, Direction: dir, Peer: c.PeerJid,
			StartedAt: time.Now().UnixMilli(), Status: mapStatus(c.StateData.State),
		}
		if existing != nil {
			rec.Owner = existing.Owner
			rec.StartedAt = existing.StartedAt
		}
		s.mgr.broker.upsertCall(rec)

		if c.StateData.State == core.CallStateActive {
			ac, acOk := s.reg.get(c.CallID)
			holdURL := ""
			holdCancel := false
			if acOk {
				holdURL = ac.holdMusicURL
				holdCancel = ac.holdMusicCancel == nil
			}
			s.log.Info("[DEBUG] OnStateChange ACTIVE — checking hold music", "call_id", c.CallID, "ac_found", acOk, "holdMusicURL", holdURL, "cancel_nil", holdCancel)

			if acOk && ac.holdMusicURL != "" && ac.holdMusicCancel == nil {
				go func() {
					holdCtx, holdCancel := context.WithCancel(context.Background())
					ac.holdMusicCancel = holdCancel
					s.log.Info("[HOLD-MUSIC] >>> outgoing hold music starting", "call_id", c.CallID, "url", ac.holdMusicURL)
					if err := ac.cm.PlayHoldMusic(holdCtx, ac.holdMusicURL); err != nil {
						s.log.Warn("[HOLD-MUSIC] outgoing hold music ended", "call_id", c.CallID, "err", err)
					} else {
						s.log.Info("[HOLD-MUSIC] <<< outgoing hold music stopped", "call_id", c.CallID)
					}
				}()
			} else {
				s.log.Warn("[DEBUG] OnStateChange ACTIVE — hold music NOT started", "call_id", c.CallID, "ac_found", acOk, "holdMusicURL_empty", holdURL == "", "cancel_not_nil", !holdCancel)
			}
		}
	}
	cm.OnEnded = func(c *call.CallInfo) {
		if ac, ok := s.reg.get(c.CallID); ok && ac.callTimeout != nil {
			ac.callTimeout.Stop()
		}
		s.removeCall(c.CallID)
		s.mgr.broker.endCall(c.CallID, string(c.StateData.EndReason))
	}
	cm.OnPeerAudio = func(pcm16 []float32) {
		ac, ok := s.reg.get(callID)
		if !ok || ac.bridge == nil {
			return
		}
		err := ac.bridge.WritePCM(pcm16)
		if err != nil {
			s.log.Debug("[AUDIO_BRIDGE] write error", "call_id", callID, "err", err, "samples", len(pcm16))
		}
	}
}

func (s *Session) startOutgoing(ctx context.Context, peer types.JID, isVideo bool, holdMusicURL string) (string, error) {
	callID := signaling.GenerateCallID()
	cm := s.createCall(callID)
	if ac, ok := s.reg.get(callID); ok && holdMusicURL != "" {
		ac.holdMusicURL = holdMusicURL
		s.log.Info("[DEBUG] startOutgoing holdMusicURL stored in activeCall", "call_id", callID, "holdMusicURL", holdMusicURL)
	} else {
		s.log.Warn("[DEBUG] startOutgoing activeCall NOT FOUND or holdMusicURL empty", "call_id", callID, "holdMusicURL", holdMusicURL)
	}
	if err := cm.StartCall(ctx, callID, peer, isVideo); err != nil {
		s.log.Error("[DEBUG] startOutgoing StartCall FAILED", "call_id", callID, "err", err)
		s.removeCall(callID)
		return "", err
	}
	s.log.Info("[DEBUG] startOutgoing StartCall OK", "call_id", callID, "peer", peer.String())
	return callID, nil
}

func (s *Session) callForEvent(from types.JID, data *waBinary.Node) (*activeCall, bool) {
	callID := callIDFromNode(wrapCall(from, data))
	if callID == "" {
		return nil, false
	}
	return s.reg.get(callID)
}

func (s *Session) onIncomingOffer(ctx context.Context, evt *events.CallOffer) {
	node := wrapCall(evt.From, evt.Data)
	callID := callIDFromNode(node)
	if callID == "" {
		return
	}
	if max := s.mgr.maxCalls; max > 0 && s.reg.count() >= max {
		s.rejectOffer(ctx, node, evt.From)
		return
	}
	cm := s.createCall(callID)
	cm.HandleCallOffer(ctx, node, evt.From)

	if ac, ok := s.reg.get(callID); ok {
		ac.callTimeout = time.AfterFunc(30*time.Second, func() {
			s.log.Info("[CALL] inbound timeout, auto-rejecting", "call_id", callID)
			_ = cm.EndCall(context.Background(), core.EndCallReasonTimeout)
			s.removeCall(callID)
			s.mgr.broker.endCall(callID, string(core.EndCallReasonTimeout))
		})
	}
}

func (s *Session) rejectOffer(ctx context.Context, node *waBinary.Node, from types.JID) {
	info := signaling.ExtractNodeInfo(node)
	if info == nil {
		return
	}
	creator := wanode.AttrString(info.InnerNode.Attrs, "call-creator")
	if creator == "" {
		creator = from.String()
	}
	reject := signaling.BuildRejectStanza(from, info.CallID, wanode.MustJID(creator))
	_ = wa.NewSocket(s.client).SendNode(ctx, reject)
	s.log.Info("inbound call rejected: session at capacity", "call_id", info.CallID)
}

func (s *Session) handleEvent(rawEvt any) {
	ctx := context.Background()
	switch evt := rawEvt.(type) {
	case *events.Connected:
		if id := s.client.Store.ID; id != nil {
			_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
		}
		s.setAuth(AuthSnapshot{State: "open", Paired: true})
	case *events.LoggedOut:
		s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
	case *events.CallOffer:
		s.onIncomingOffer(ctx, evt)
	case *events.CallAccept:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallAccept(ctx, wrapCall(evt.From, evt.Data), evt.From)
			// Stop the inbound call timeout if the accept transitioned the state
			// (i.e. the call moved from incoming_ringing to connecting).
			if ac.callTimeout != nil {
				ac.callTimeout.Stop()
				ac.callTimeout = nil
			}
		}
	case *events.CallTransport:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTransport(ctx, wrapCall(evt.From, evt.Data), evt.From)
		}
	case *events.CallTerminate:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTerminate(wrapCall(evt.From, evt.Data))
		}
	case *events.CallReject:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTerminate(wrapCall(evt.From, evt.Data))
		}
	}
}

func (s *Session) connect(ctx context.Context) error {
	if s.client.Store.ID != nil {
		return s.client.Connect()
	}
	return s.startPairing(ctx)
}

func (s *Session) startPairing(ctx context.Context) error {
	s.log.Info("startPairing called", "session", s.id)
	qrChan, err := s.client.GetQRChannel(ctx)
	if err != nil {
		s.log.Error("GetQRChannel failed", "session", s.id, "err", err)
		return err
	}
	s.log.Info("GetQRChannel OK", "session", s.id)
	// Log do estado do device antes do Connect
	if s.client.Store != nil {
		s.log.Info("device info before Connect", "session", s.id, "storeID", s.client.Store.ID)
	} else {
		s.log.Info("device store is nil before Connect", "session", s.id)
	}
	if err := s.client.Connect(); err != nil {
		s.log.Error("Connect failed", "session", s.id, "err", err)
		return err
	}
	s.log.Info("Connect OK", "session", s.id)
	// Log do estado do device depois do Connect
	if s.client.Store != nil {
		s.log.Info("device info after Connect", "session", s.id, "storeID", s.client.Store.ID)
	}
	go func() {
		for evt := range qrChan {
			s.log.Info("QR channel event", "session", s.id, "event", evt.Event)
			switch evt.Event {
			case "code":
				s.log.Info("scan the QR code to pair this session", "session", s.id)
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				s.setAuth(AuthSnapshot{State: "qr", QR: evt.Code})
				s.mgr.broker.emitSessionQR(s.id, evt.Code)
			case "success":
				if id := s.client.Store.ID; id != nil {
					_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
				}
				s.setAuth(AuthSnapshot{State: "open", Paired: true})
			case "timeout":
				s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
			}
		}
		s.log.Info("QR channel closed", "session", s.id)
	}()
	return nil
}

func (s *Session) setAuth(a AuthSnapshot) {
	s.mu.Lock()
	s.auth = a
	s.mu.Unlock()
	s.mgr.broker.emitAuthState(s.id, a)
	s.mgr.broker.emitSessionList(s.mgr.infos())
}

func (s *Session) info() SessionInfo {
	s.mu.Lock()
	a := s.auth
	s.mu.Unlock()
	jid := ""
	if id := s.client.Store.ID; id != nil {
		jid = id.String()
	}
	return SessionInfo{ID: s.id, Name: s.name, JID: jid, State: a.State, Paired: a.Paired || jid != ""}
}

func (s *Session) setBridge(callID string, b *Bridge) {
	oldB, found := s.reg.setBridge(callID, b)
	if !found {
		b.Close()
		return
	}
	if oldB != nil {
		oldB.Close()
	}
}

func (s *Session) removeCall(callID string) {
	ac, ok := s.reg.remove(callID)
	if !ok {
		return
	}
	if ac.bridge != nil {
		ac.bridge.Close()
	}
}

func (s *Session) terminateCall(callID string, reason core.EndCallReason) {
	ac, ok := s.reg.get(callID)
	if !ok {
		return
	}
	_ = ac.cm.EndCall(context.Background(), reason)
}

func (s *Session) teardownAllCalls() {
	for _, ac := range s.reg.drain() {
		_ = ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
		if ac.bridge != nil {
			ac.bridge.Close()
		}
	}
}

func (s *Session) replaceClient(client *whatsmeow.Client) {
	s.teardownAllCalls()
	s.client.Disconnect()
	s.client = client
	client.AddEventHandler(s.handleEvent)
}

func (s *Session) shutdown() {
	s.teardownAllCalls()
	s.client.Disconnect()
}

func (s *Session) cleanupOrphanedCalls() {
	now := time.Now()
	calls := s.reg.activeCalls()
	for callID, startedAt := range calls {
		age := now.Sub(startedAt)
		if age > callMaxDuration {
			s.log.Warn("[WATCHDOG] ending orphaned call", "call_id", callID, "age", age.Round(time.Second))
			s.terminateCall(callID, core.EndCallReasonTimeout)
		}
	}
}

func mapStatus(state core.CallState) CallStatus {
	switch state {
	case core.CallStateActive:
		return StatusConnected
	case core.CallStateEnded:
		return StatusEnded
	case core.CallStateInitiating:
		return StatusStarting
	default:
		return StatusRinging
	}
}
