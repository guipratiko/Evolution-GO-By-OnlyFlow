package whatsmeow_service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	// whatsmeow KeepAliveMaxFailTime is 3m; reconnect a bit earlier once failures persist.
	keepAliveFailReconnectAfter = 2 * time.Minute
	sessionProbeInterval        = 2 * time.Minute
	sessionProbeTimeout         = 20 * time.Second
	maxRecoveryAttempts         = 2
	recoveryAttemptResetAfter   = 30 * time.Minute
	// Some companion sessions go half-open around ~15–24h without a clean Disconnected.
	// Refresh before that window to reduce zombie inbound stalls.
	sessionMaxUptime = 12 * time.Hour

	// Half-open pattern: inbound Message stalls, then a send wakes WA and a backlog flushes.
	// Arm watch only if we already saw inbound traffic this session and it went quiet.
	inboundSilenceArmWatch = 8 * time.Minute
	flushWatchWindow       = 25 * time.Second
	flushBurstMinCount     = 2
	flushStaleMinCount     = 1
	flushStaleMessageAge   = 90 * time.Second

	// Inbound-dead companion: connected + can send, but no *events.Message (!IsFromMe).
	inboundDeadAfter     = 25 * time.Minute
	inboundDeadMinUptime = 20 * time.Minute
)

// sessionRecoveryTracker keeps recovery progress across ReconnectClient cycles
// (MyClient is recreated on every reconnect).
type sessionRecoveryTracker struct {
	mu                   sync.Mutex
	inProgress           map[string]bool
	attempts             map[string]int
	lastAttempt          map[string]time.Time
	lastInboundMessageAt map[string]int64
	lastOutboundSendAt   map[string]int64
	reconnectCount       map[string]int
}

func newSessionRecoveryTracker() *sessionRecoveryTracker {
	return &sessionRecoveryTracker{
		inProgress:           make(map[string]bool),
		attempts:             make(map[string]int),
		lastAttempt:          make(map[string]time.Time),
		lastInboundMessageAt: make(map[string]int64),
		lastOutboundSendAt:   make(map[string]int64),
		reconnectCount:       make(map[string]int),
	}
}

func (t *sessionRecoveryTracker) tryBegin(instanceID string) (attempts int, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.inProgress[instanceID] {
		return t.attempts[instanceID], false
	}

	if last, exists := t.lastAttempt[instanceID]; exists && time.Since(last) > recoveryAttemptResetAfter {
		t.attempts[instanceID] = 0
	}

	t.inProgress[instanceID] = true
	t.attempts[instanceID]++
	t.lastAttempt[instanceID] = time.Now()
	return t.attempts[instanceID], true
}

func (t *sessionRecoveryTracker) end(instanceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inProgress, instanceID)
}

// resetAttempts clears escalation counters but keeps inbound/outbound evidence.
func (t *sessionRecoveryTracker) resetAttempts(instanceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inProgress, instanceID)
	delete(t.attempts, instanceID)
	delete(t.lastAttempt, instanceID)
}

func (t *sessionRecoveryTracker) reset(instanceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inProgress, instanceID)
	delete(t.attempts, instanceID)
	delete(t.lastAttempt, instanceID)
	delete(t.lastInboundMessageAt, instanceID)
	delete(t.lastOutboundSendAt, instanceID)
	delete(t.reconnectCount, instanceID)
}

func (t *sessionRecoveryTracker) noteInbound(instanceID string, at time.Time) {
	if t == nil || instanceID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastInboundMessageAt[instanceID] = at.UnixNano()
}

func (t *sessionRecoveryTracker) noteOutbound(instanceID string, at time.Time) {
	if t == nil || instanceID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastOutboundSendAt[instanceID] = at.UnixNano()
}

func (t *sessionRecoveryTracker) noteReconnect(instanceID string) {
	if t == nil || instanceID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reconnectCount[instanceID]++
}

func (t *sessionRecoveryTracker) traffic(instanceID string) (lastInbound, lastOutbound int64, reconnects int) {
	if t == nil || instanceID == "" {
		return 0, 0, 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastInboundMessageAt[instanceID], t.lastOutboundSendAt[instanceID], t.reconnectCount[instanceID]
}

func (mycli *MyClient) touchLastEvent() {
	if mycli == nil {
		return
	}
	mycli.lastEventAt.Store(time.Now().UnixNano())
}

func (mycli *MyClient) markKeepAliveFailing(failing bool) {
	if mycli == nil {
		return
	}
	mycli.keepAliveFailing.Store(failing)
	if failing && mycli.keepAliveFailSince.Load() == 0 {
		mycli.keepAliveFailSince.Store(time.Now().UnixNano())
	}
	if !failing {
		mycli.keepAliveFailSince.Store(0)
	}
}

func (mycli *MyClient) stopSessionWatchdog() {
	if mycli == nil {
		return
	}
	mycli.watchdogStopOnce.Do(func() {
		close(mycli.watchdogStop)
	})
}

func (mycli *MyClient) startSessionWatchdog() {
	if mycli == nil {
		return
	}
	if !mycli.watchdogStarted.CompareAndSwap(false, true) {
		return
	}
	mycli.hydrateTrafficFromTracker()
	go mycli.sessionWatchdogLoop()
}

func (mycli *MyClient) hydrateTrafficFromTracker() {
	if mycli == nil || mycli.service == nil {
		return
	}
	tracker := mycli.service.SessionRecovery()
	if tracker == nil {
		return
	}
	lastIn, lastOut, _ := tracker.traffic(mycli.userID)
	if lastIn > 0 && mycli.lastInboundMessageAt.Load() == 0 {
		mycli.lastInboundMessageAt.Store(lastIn)
	}
	if lastOut > 0 {
		// outbound is tracker-authoritative across reconnects
		_ = lastOut
	}
}

func (mycli *MyClient) sessionWatchdogLoop() {
	ticker := time.NewTicker(sessionProbeInterval)
	defer ticker.Stop()

	mycli.loggerWrapper.GetLogger(mycli.userID).LogInfo(
		"[%s] Session watchdog started (probe every %s)",
		mycli.userID, sessionProbeInterval,
	)

	for {
		select {
		case <-ticker.C:
			if !mycli.runSessionHealthCheck() {
				return
			}
		case <-mycli.watchdogStop:
			mycli.loggerWrapper.GetLogger(mycli.userID).LogInfo("[%s] Session watchdog stopped", mycli.userID)
			return
		}
	}
}

// runSessionHealthCheck returns false when the watchdog should exit.
func (mycli *MyClient) runSessionHealthCheck() bool {
	if mycli == nil || mycli.WAClient == nil {
		return false
	}

	if _, err := mycli.instanceRepository.GetInstanceByID(mycli.userID); err != nil {
		mycli.loggerWrapper.GetLogger(mycli.userID).LogInfo("[%s] Instance gone, stopping session watchdog", mycli.userID)
		return false
	}

	if !mycli.WAClient.IsLoggedIn() {
		return true
	}

	if !mycli.WAClient.IsConnected() {
		return true
	}

	if connectedSince := mycli.connectedSince.Load(); connectedSince > 0 {
		uptime := time.Since(time.Unix(0, connectedSince))
		if uptime >= sessionMaxUptime {
			mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
				"[%s] Session uptime %s >= %s — preventive recovery (counts toward escalation)",
				mycli.userID, uptime.Round(time.Minute), sessionMaxUptime,
			)
			// Reset before triggering so we don't loop every tick if reconnect is slow.
			mycli.connectedSince.Store(time.Now().UnixNano())
			go mycli.forceSessionRecovery("session_ttl")
			return true
		}
	}

	if mycli.keepAliveFailing.Load() {
		failSince := mycli.keepAliveFailSince.Load()
		if failSince > 0 && time.Since(time.Unix(0, failSince)) >= keepAliveFailReconnectAfter {
			mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
				"[%s] KeepAlive failing for >= %s — forcing session recovery",
				mycli.userID, keepAliveFailReconnectAfter,
			)
			go mycli.forceSessionRecovery("keepalive_timeout")
			return true
		}
	}

	if err := mycli.probeSessionLiveness(); err != nil {
		mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
			"[%s] Session liveness probe failed: %v — forcing session recovery",
			mycli.userID, err,
		)
		go mycli.forceSessionRecovery("liveness_probe_failed")
		return true
	}

	if mycli.shouldRecoverInboundDead("watchdog") {
		go mycli.forceSessionRecovery("inbound_dead_after_outbound")
	}

	return true
}

func (mycli *MyClient) probeSessionLiveness() error {
	if mycli.WAClient == nil || mycli.WAClient.Store == nil || mycli.WAClient.Store.ID == nil {
		return fmt.Errorf("client store not ready")
	}

	ctx, cancel := context.WithTimeout(context.Background(), sessionProbeTimeout)
	defer cancel()

	own := mycli.WAClient.Store.ID.ToNonAD()
	_, err := mycli.WAClient.GetUserInfo(ctx, []types.JID{own})
	return err
}

func (mycli *MyClient) touchLastInboundMessage() {
	if mycli == nil {
		return
	}
	now := time.Now()
	mycli.lastInboundMessageAt.Store(now.UnixNano())
	if mycli.service != nil {
		if tracker := mycli.service.SessionRecovery(); tracker != nil {
			tracker.noteInbound(mycli.userID, now)
		}
	}
}

// noteInboundMessage tracks inbound Message health and half-open flush-after-send bursts.
func (mycli *MyClient) noteInboundMessage(evt *events.Message) {
	if mycli == nil || evt == nil {
		return
	}

	// Own echoes / sync of sent messages do not prove the inbound fanout is alive.
	if evt.Info.IsFromMe {
		return
	}

	mycli.touchLastInboundMessage()

	watchUntil := mycli.outboundWatchUntil.Load()
	if watchUntil == 0 || time.Now().UnixNano() > watchUntil {
		return
	}

	burst := mycli.flushBurstCount.Add(1)
	stale := mycli.flushStaleCount.Load()
	if !evt.Info.Timestamp.IsZero() && time.Since(evt.Info.Timestamp) >= flushStaleMessageAge {
		stale = mycli.flushStaleCount.Add(1)
	}

	mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
		"[%s] Possible inbound flush after send — burst=%d stale=%d msgAge=%s id=%s from=%s",
		mycli.userID, burst, stale, time.Since(evt.Info.Timestamp).Round(time.Second),
		evt.Info.ID, evt.Info.Chat.String(),
	)
}

// armOutboundFlushWatch starts a short window to detect backlog flush after an OnlyFlow send.
func (mycli *MyClient) armOutboundFlushWatch() {
	if mycli == nil {
		return
	}

	lastInbound := mycli.effectiveLastInbound()
	if lastInbound == 0 {
		// No Message seen yet — inbound-dead timer covers this path instead.
		return
	}

	silence := time.Since(time.Unix(0, lastInbound))
	if silence < inboundSilenceArmWatch {
		return
	}

	mycli.outboundWatchUntil.Store(time.Now().Add(flushWatchWindow).UnixNano())
	mycli.silenceBeforeOutbound.Store(int64(silence))
	mycli.flushBurstCount.Store(0)
	mycli.flushStaleCount.Store(0)
	mycli.flushReconnectScheduled.Store(false)

	mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
		"[%s] Armed outbound flush watch — inbound silence %s (window %s)",
		mycli.userID, silence.Round(time.Second), flushWatchWindow,
	)

	go func() {
		timer := time.NewTimer(flushWatchWindow + 500*time.Millisecond)
		defer timer.Stop()
		<-timer.C
		mycli.evaluateOutboundFlushWatch()
	}()
}

func (mycli *MyClient) armInboundDeadWatch(outboundAt time.Time) {
	if mycli == nil {
		return
	}

	go func(sentAt time.Time) {
		timer := time.NewTimer(inboundDeadAfter + time.Second)
		defer timer.Stop()
		<-timer.C

		lastIn := mycli.effectiveLastInbound()
		if lastIn > sentAt.UnixNano() {
			return
		}
		if mycli.shouldRecoverInboundDead("post_outbound_timer") {
			mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
				"[%s] Inbound still dead %s after outbound — forcing recovery",
				mycli.userID, inboundDeadAfter,
			)
			mycli.forceSessionRecovery("inbound_dead_after_outbound")
		}
	}(outboundAt)
}

func (mycli *MyClient) effectiveLastInbound() int64 {
	if mycli == nil {
		return 0
	}
	local := mycli.lastInboundMessageAt.Load()
	if mycli.service == nil {
		return local
	}
	tracker := mycli.service.SessionRecovery()
	if tracker == nil {
		return local
	}
	tracked, _, _ := tracker.traffic(mycli.userID)
	if tracked > local {
		mycli.lastInboundMessageAt.Store(tracked)
		return tracked
	}
	return local
}

// shouldRecoverInboundDead detects companions that stay connected/sendable without inbound Message.
func (mycli *MyClient) shouldRecoverInboundDead(source string) bool {
	if mycli == nil || mycli.WAClient == nil {
		return false
	}
	if !mycli.WAClient.IsLoggedIn() || !mycli.WAClient.IsConnected() {
		return false
	}

	connectedSince := mycli.connectedSince.Load()
	if connectedSince == 0 {
		return false
	}
	uptime := time.Since(time.Unix(0, connectedSince))
	if uptime < inboundDeadMinUptime {
		return false
	}

	var lastOut int64
	var reconnects int
	if mycli.service != nil {
		if tracker := mycli.service.SessionRecovery(); tracker != nil {
			_, lastOut, reconnects = tracker.traffic(mycli.userID)
		}
	}
	if lastOut == 0 {
		return false
	}

	lastIn := mycli.effectiveLastInbound()
	now := time.Now()
	outboundAge := now.Sub(time.Unix(0, lastOut))

	if lastIn == 0 {
		// Never saw inbound Message (persisted across soft reconnects).
		// Require outbound to be old enough; reconnects>0 makes this a stronger zombie signal
		// after an earlier soft recovery already failed to restore inbound.
		if outboundAge < inboundDeadAfter {
			return false
		}
		if reconnects < 1 && uptime < inboundDeadMinUptime+inboundDeadAfter {
			// Brand-new quiet sender: wait a bit longer before first auto-recovery.
			return false
		}
		mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
			"[%s] Inbound-dead suspect (%s): never received Message, reconnects=%d, lastOutboundAge=%s, uptime=%s",
			mycli.userID, source, reconnects, outboundAge.Round(time.Second), uptime.Round(time.Second),
		)
		return true
	}

	silence := now.Sub(time.Unix(0, lastIn))
	if silence < inboundDeadAfter {
		return false
	}
	// Outbound after inbound went quiet → classic half-open / zombie.
	if lastOut <= lastIn {
		return false
	}
	// post_outbound_timer already waited inboundDeadAfter after the send.
	if source == "watchdog" && outboundAge < inboundDeadAfter {
		return false
	}

	mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
		"[%s] Inbound-dead suspect (%s): silence=%s, outboundAfterInbound=true, uptime=%s",
		mycli.userID, source, silence.Round(time.Second), uptime.Round(time.Second),
	)
	return true
}

func (mycli *MyClient) evaluateOutboundFlushWatch() {
	if mycli == nil {
		return
	}

	burst := mycli.flushBurstCount.Load()
	stale := mycli.flushStaleCount.Load()
	silenceNs := mycli.silenceBeforeOutbound.Load()
	mycli.outboundWatchUntil.Store(0)

	triggered := stale >= flushStaleMinCount ||
		(burst >= flushBurstMinCount && stale >= 1)

	if !triggered {
		if burst > 0 {
			mycli.loggerWrapper.GetLogger(mycli.userID).LogInfo(
				"[%s] Outbound flush watch ended — burst=%d stale=%d (below threshold)",
				mycli.userID, burst, stale,
			)
		}
		return
	}

	if !mycli.flushReconnectScheduled.CompareAndSwap(false, true) {
		return
	}

	mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
		"[%s] Inbound backlog flush detected after send (silence=%s burst=%d stale=%d) — forcing recovery (reconnect may not heal this companion)",
		mycli.userID, time.Duration(silenceNs).Round(time.Second), burst, stale,
	)
	// Soft reconnect is not enough for some companion sessions (e.g. persistent DDD-34 zombies);
	// count toward escalation so repeated flushes end in logout + re-pair.
	go mycli.forceSessionRecovery("inbound_flush_after_send")
}

// NotifyOutboundSend arms flush-after-send and inbound-dead detection for half-open sessions.
func (w *whatsmeowService) NotifyOutboundSend(instanceID string) {
	if w == nil || instanceID == "" {
		return
	}
	mycli, ok := w.myClientPointer[instanceID]
	if !ok || mycli == nil {
		return
	}

	now := time.Now()
	if tracker := w.SessionRecovery(); tracker != nil {
		tracker.noteOutbound(instanceID, now)
	}
	mycli.armOutboundFlushWatch()
	mycli.armInboundDeadWatch(now)
}

func (mycli *MyClient) forceSessionRecovery(reason string) {
	if mycli == nil || mycli.service == nil {
		return
	}

	tracker := mycli.service.SessionRecovery()
	if tracker == nil {
		return
	}

	attempts, ok := tracker.tryBegin(mycli.userID)
	if !ok {
		mycli.loggerWrapper.GetLogger(mycli.userID).LogInfo(
			"[%s] Session recovery already in progress (reason=%s)", mycli.userID, reason,
		)
		return
	}
	defer tracker.end(mycli.userID)

	mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
		"[%s] ===== SESSION RECOVERY ===== reason=%s attempt=%d/%d",
		mycli.userID, reason, attempts, maxRecoveryAttempts,
	)

	mycli.emitConnectionEvent("SessionUnhealthy", map[string]interface{}{
		"reason":   reason,
		"attempt":  attempts,
		"maxTries": maxRecoveryAttempts,
		"action":   "reconnect",
	})

	if attempts > maxRecoveryAttempts {
		mycli.loggerWrapper.GetLogger(mycli.userID).LogError(
			"[%s] Session recovery exhausted (%d attempts) — forcing logout for re-pair",
			mycli.userID, attempts,
		)
		mycli.escalateSessionLogout(reason)
		tracker.reset(mycli.userID)
		return
	}

	tracker.noteReconnect(mycli.userID)

	if err := mycli.service.ReconnectClient(mycli.userID); err != nil {
		mycli.loggerWrapper.GetLogger(mycli.userID).LogError(
			"[%s] Session recovery reconnect failed: %v", mycli.userID, err,
		)
	}
}

func (mycli *MyClient) escalateSessionLogout(reason string) {
	if mycli.WAClient == nil {
		return
	}

	mycli.emitConnectionEvent("SessionUnhealthy", map[string]interface{}{
		"reason":  reason,
		"action":  "logout",
		"message": "zombie session could not be healed; re-pair required",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if mycli.WAClient.IsLoggedIn() {
		if err := mycli.WAClient.Logout(ctx); err != nil {
			mycli.loggerWrapper.GetLogger(mycli.userID).LogError(
				"[%s] Forced logout failed during session escalate: %v", mycli.userID, err,
			)
			mycli.WAClient.Disconnect()
		}
	} else if mycli.WAClient.IsConnected() {
		mycli.WAClient.Disconnect()
	}

	mycli.Instance.Connected = false
	mycli.Instance.DisconnectReason = fmt.Sprintf("session_recovery_logout:%s", reason)
	mycli.Instance.Jid = ""

	if err := mycli.instanceRepository.UpdateJid(mycli.userID, ""); err != nil {
		mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn("[%s] Failed to clear jid: %v", mycli.userID, err)
	}
	if err := mycli.instanceRepository.UpdateConnected(mycli.userID, false, mycli.Instance.DisconnectReason); err != nil {
		mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn("[%s] Failed to update connected=false: %v", mycli.userID, err)
	}

	select {
	case mycli.killChannel[mycli.userID] <- true:
	default:
		mycli.loggerWrapper.GetLogger(mycli.userID).LogWarn(
			"[%s] Kill channel full/unavailable after session escalate", mycli.userID,
		)
	}
}

func (mycli *MyClient) emitConnectionEvent(event string, data map[string]interface{}) {
	if mycli == nil || mycli.Instance == nil {
		return
	}

	postMap := map[string]interface{}{
		"event":         event,
		"data":          data,
		"instanceToken": mycli.token,
		"instanceId":    mycli.userID,
		"instanceName":  mycli.Instance.Name,
	}

	values, err := json.Marshal(postMap)
	if err != nil {
		mycli.loggerWrapper.GetLogger(mycli.userID).LogError("[%s] Failed to marshal %s event: %v", mycli.userID, event, err)
		return
	}

	queueName := strings.ToLower(fmt.Sprintf("%s.%s", mycli.userID, event))
	mycli.loggerWrapper.GetLogger(mycli.userID).LogInfo(
		"[%s] ===== DISPATCHING WEBHOOK ===== Event: %s, Queue: %s", mycli.userID, event, queueName,
	)

	instance := mycli.Instance
	go mycli.service.CallWebhook(instance, queueName, values)

	if mycli.config != nil && (mycli.config.AmqpGlobalEnabled || mycli.config.NatsGlobalEnabled) {
		go mycli.service.SendToGlobalQueues(event, values, mycli.userID)
	}
}

// SessionRecovery exposes the shared tracker for MyClient recovery flows.
func (w *whatsmeowService) SessionRecovery() *sessionRecoveryTracker {
	if w == nil {
		return nil
	}
	return w.sessionRecovery
}

// MarkSessionHealthy resets escalation counters after inbound proves the session is alive.
// Inbound/outbound timestamps are kept so a later zombie can still be detected across reconnects.
func (w *whatsmeowService) MarkSessionHealthy(instanceID string) {
	if w == nil || w.sessionRecovery == nil {
		return
	}
	w.sessionRecovery.resetAttempts(instanceID)
}
