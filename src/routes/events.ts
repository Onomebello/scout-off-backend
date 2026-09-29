import { Router, Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import {
  broadcaster,
  SseSubscriber,
  SseFilterCriteria,
  BroadcastEvent,
} from '../services/eventBroadcaster';
import { CONTRACT_EVENT_TYPES, ContractEventType } from '../types';
import { logger } from '../utils/logger';
import { ErrorCode } from '../utils/errorCodes';
import {
  isWalletBlocklisted,
  refreshBlockedWallets,
  onWalletBlocked,
} from '../services/walletBlocklist';
import * as tokenBlocklistModule from '../services/tokenBlocklist';

const router = Router();

/** Seconds a client should wait before retrying when the SSE connection limit is hit. */
const SSE_CAPACITY_RETRY_AFTER_SECONDS = 30;

// ─── Configuration ────────────────────────────────────────────────────────────

/** Interval between keep-alive comment pings, in milliseconds. */
const KEEPALIVE_INTERVAL_MS = parseInt(
  process.env.SSE_KEEPALIVE_INTERVAL_MS ?? '15000',
  10,
);

/**
 * Interval for the shared authorization sweep, in milliseconds.
 *
 * The sweep re-checks the token-revocation blocklist and wallet blocklist in
 * a SINGLE query per process (never one per connection or per keep-alive
 * tick) so revocations/blocklists that were persisted by another backend
 * instance are detected within this bound. In-process revocations are
 * delivered synchronously via event listeners and take effect immediately.
 *
 * Documented detection bound (see docs/auth.md):
 *   - same-process revocation/blocklist: immediate (synchronous event)
 *   - cross-process: ≤ SSE_AUTH_SWEEP_INTERVAL_MS (default 30 000 ms)
 */
const AUTH_SWEEP_INTERVAL_MS = parseInt(
  process.env.SSE_AUTH_SWEEP_INTERVAL_MS ?? '30000',
  10,
);

/** Maximum number of concurrent SSE connections (0 = unlimited). Read live
 *  (not cached at module load) so tests can flip it per-case. */
function getMaxSseConnections(): number {
  return parseInt(process.env.SSE_MAX_CONNECTIONS ?? '0', 10);
}

// ─── Valid event type set (for query param validation) ────────────────────────

const VALID_EVENT_TYPES = new Set<ContractEventType>(CONTRACT_EVENT_TYPES);

// ─── SSE frame helpers ───────────────────────────────────────────────────────

/**
 * Serialise a BroadcastEvent to an SSE frame.
 *
 * SSE format:
 *   event: <type>\n
 *   data: <json>\n
 *   \n
 */
function formatSseFrame(event: BroadcastEvent): string {
  const data = JSON.stringify({ type: event.type, payload: event.payload });
  return `event: ${event.type}\ndata: ${data}\n\n`;
}

/** SSE keep-alive comment frame — ignored by the EventSource API but prevents
 *  proxy/load-balancer timeouts on idle connections. */
const KEEPALIVE_FRAME = ': ping\n\n';

// ─── Bounded authorization sweep (one interval per process) ──────────────────

/**
 * Active, connected sessions. Each entry carries the auth state needed to
 * terminate the stream (jti, wallet) plus the subscriber itself.
 * The entry is added by the route handler and removed in cleanup().
 */
interface ActiveSession {
  wallet: string;
  jti: string | undefined;
  subscriber: SseSubscriber;
  /** Terminate the connection; safe to call more than once. */
  terminate: (reason: 'token_revoked' | 'wallet_blocklisted') => void;
}

/** Sessions currently open in this process. */
const activeSessions = new Set<ActiveSession>();

/** Sweep body shared by the interval and tests. */
export async function runAuthorizationSweep(): Promise<void> {
  if (activeSessions.size === 0) return;

  // Single query regardless of connection count — never per keep-alive tick.
  let revokedJtis: ReadonlySet<string>;
  try {
    revokedJtis = new Set(await tokenBlocklistModule.getActiveRevokedJtis());
  } catch {
    revokedJtis = new Set();
  }

  let blockedWallets: ReadonlySet<string>;
  try {
    blockedWallets = new Set(await refreshBlockedWallets());
  } catch {
    blockedWallets = new Set();
  }

  for (const session of activeSessions) {
    if (session.jti && revokedJtis.has(session.jti)) {
      session.terminate('token_revoked');
    } else if (blockedWallets.has(session.wallet)) {
      session.terminate('wallet_blocklisted');
    }
  }
}

// Started lazily on first connection; unref()ed so it never keeps the process
// alive; skips all work when no SSE sessions are open.
let authSweepTimer: NodeJS.Timeout | null = null;

// ─── Route ────────────────────────────────────────────────────────────────────

/**
 * GET /api/events/stream
 *
 * Server-Sent Events endpoint. Opens a long-lived HTTP connection and pushes
 * relevant contract events to the authenticated client as they are indexed.
 *
 * Authentication: Bearer JWT (same as all other protected routes).
 *
 * Query parameters (all optional, combinable):
 *   - eventType  One event type name to subscribe to (e.g. "milestone_approved").
 *                When omitted the client receives all event types that pass the
 *                wallet-relevance filter.  Unknown values are ignored.
 *   - playerId   Only deliver events whose payload contains this player identifier.
 *                When omitted no additional player-level filtering is applied.
 *
 * Filtering: only events relevant to the authenticated wallet are sent (wallet
 * isolation is always enforced regardless of query params).  The optional
 * query params add further narrowing on top.
 *
 * SSE event types sent:
 *   - milestone_approved  (player: their own milestone approvals)
 *   - scout_subscribed    (scout: their own subscription changes)
 *   - contact_unlocked    (scout: their own contact unlocks)
 *   - trial_offer_logged  (scout/player: trial offers involving them)
 *   - player_registered   (player: their own registration)
 *   - milestone_submitted (player/validator)
 *   - fees_withdrawn      (admin)
 *
 * Live authorization enforcement (#1019):
 *   - If the authenticated JWT is revoked (via POST /auth/logout or admin
 *     token revocation) while the stream is open, the connection emits a
 *     terminal `session_ended` event (reason "token_revoked") and closes;
 *     no further protected events are delivered.
 *   - If the wallet is blocklisted while the stream is open, the same
 *     termination happens with reason "wallet_blocklisted".
 *   - Detection bound: immediate for revocations/blocklists processed in
 *     this process; ≤ SSE_AUTH_SWEEP_INTERVAL_MS (default 30 s) for changes
 *     persisted by another instance (one sweep query per process, never a
 *     DB query per keep-alive tick).
 *   - Blocklisted wallets cannot open a new connection (403).
 *
 * Keep-alive: a `: ping` comment is sent every SSE_KEEPALIVE_INTERVAL_MS ms
 * (default 15 s) to prevent idle-connection timeouts.
 *
 * @auth Bearer token required (any role)
 * @response 200 text/event-stream — long-lived SSE connection
 * @response 401 { success: false, error: string } — missing or invalid token
 * @response 403 { success: false, error: string, code: 'WALLET_BLOCKLISTED' } — wallet is blocklisted
 * @response 503 { success: false, error: string, code: 'SSE_CAPACITY' } — connection limit reached (sets Retry-After)
 */
router.get('/stream', requireAuth, async (req: Request, res: Response) => {
  const wallet = req.account!;

  // ── Blocklist gate: blocklisted wallets may not open a stream ────────────
  if (await isWalletBlocklisted(wallet)) {
    logger.warn(`[sse] connection rejected, wallet blocklisted=${wallet}`);
    res.status(403).json({
      success: false,
      error: 'Account is blocklisted; SSE access revoked',
      code: ErrorCode.WALLET_BLOCKLISTED,
    });
    return;
  }

  // ── Connection limit guard ─────────────────────────────────────────────────
  const maxSseConnections = getMaxSseConnections();
  if (maxSseConnections > 0 && broadcaster.subscriberCount >= maxSseConnections) {
    res.setHeader('Retry-After', String(SSE_CAPACITY_RETRY_AFTER_SECONDS));
    res.status(503).json({
      success: false,
      error: 'SSE connection limit reached. Please try again later.',
      code: ErrorCode.SSE_CAPACITY,
    });
    return;
  }

  // ── Parse optional filter query params ────────────────────────────────────
  const rawEventType = req.query.eventType as string | undefined;
  const rawPlayerId = req.query.playerId as string | undefined;

  const eventTyp

/* … truncated 4318 chars — edit only what you need near the top … */
