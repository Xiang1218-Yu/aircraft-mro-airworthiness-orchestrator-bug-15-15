import type { OutboxLease, OutboxMessage } from '../domain/types.js';
import { Store } from '../store/store.js';

const LEASE_TTL_MS = 30_000;

/** Thrown when a caller does not hold the lease (missing or wrong owner). */
export class LeaseNotHeldError extends Error {
  constructor(messageId: string) {
    super(`lease for outbox message ${messageId} is not held by caller`);
    this.name = 'LeaseNotHeldError';
  }
}

/** Thrown when a caller holds a lease but presents a stale version (fencing rejection). */
export class LeaseVersionConflictError extends Error {
  constructor(messageId: string, expected: number, actual: number) {
    super(`stale lease version for outbox message ${messageId}: expected ${expected}, current ${actual}`);
    this.name = 'LeaseVersionConflictError';
  }
}

/** Thrown when a lease has already expired and the operation requires an active lease. */
export class LeaseExpiredError extends Error {
  constructor(messageId: string) {
    super(`lease for outbox message ${messageId} has expired`);
    this.name = 'LeaseExpiredError';
  }
}

/**
 * Claims outbox messages for a consumer with a time-bounded lease.
 *
 * A message is claimable only while PENDING and either never leased or past its
 * `leaseUntil`. An unexpired lease is therefore never preempted by another consumer.
 */
export class OutboxLeaseService {
  constructor(private readonly store: Store) {}

  claim(owner: string, now: string): OutboxLease[] {
    const claimed: OutboxLease[] = [];
    const at = new Date(now).getTime();
    for (const message of this.store.outbox) {
      if (message.status !== 'PENDING') continue;
      if (message.leaseOwner !== null && message.leaseUntil !== null && new Date(message.leaseUntil).getTime() > at) {
        // Active lease held by (possibly another) consumer: do not preempt.
        continue;
      }
      this.grant(message, owner, at);
      claimed.push(this.token(message));
      this.store.addAudit(message.tenantId, 'outbox.claimed', 'outbox', message.id, {
        owner,
        attempt: message.attempts,
        version: message.version,
      });
    }
    return claimed;
  }

  /**
   * Extends a lease for a long-running publish. Requires the caller to still own an
   * unexpired lease and to present the version of the token it holds. Returns a fresh
   * token; the previous token is immediately stale and must not be used again.
   */
  renew(messageId: string, owner: string, expectedVersion: number, now: string): OutboxLease {
    const message = this.require(messageId);
    this.requireHolder(message, owner, expectedVersion);
    if (this.isExpired(message, new Date(now).getTime())) {
      // The lease has lapsed; another consumer may already have reclaimed the message.
      throw new LeaseExpiredError(messageId);
    }
    message.leaseUntil = new Date(new Date(now).getTime() + LEASE_TTL_MS).toISOString();
    message.version += 1;
    this.store.addAudit(message.tenantId, 'outbox.renewed', 'outbox', message.id, {
      owner,
      version: message.version,
      leaseUntil: message.leaseUntil,
    });
    return this.token(message);
  }

  /**
   * Confirms a successful publish.
   *
   * Guarded by the (owner, version) fencing token of the exact claim that performed the
   * publish: once a lease times out and the message is reclaimed by another consumer,
   * the version is bumped, so the original holder's confirm is rejected even if its
   * network call eventually succeeded. The PENDING -> PUBLISHED transition happens at
   * most once, hence the success audit and any downstream side effect tied to it cannot
   * be duplicated by a second "successful" publish.
   */
  confirm(messageId: string, owner: string, expectedVersion: number, now: string): void {
    const message = this.require(messageId);
    if (message.status === 'PUBLISHED') {
      // Already settled (possibly by this same owner retrying a confirm): never emit a
      // second transition or audit.
      return;
    }
    if (message.status === 'FAILED') {
      throw new LeaseNotHeldError(messageId);
    }
    this.requireHolder(message, owner, expectedVersion);
    message.status = 'PUBLISHED';
    message.publishedAt = now;
    message.leaseOwner = null;
    message.leaseUntil = null;
    message.version += 1;
    message.lastError = null;
    this.store.addAudit(message.tenantId, 'outbox.published', 'outbox', message.id, {
      owner,
      version: message.version,
      attempts: message.attempts,
    });
  }

  /**
   * Records a failed publish.
   *
   * Transient failures release the lease so the message becomes reclaimable immediately
   * (its attempt count is retained for retry policy/backoff). Terminal failures move the
   * message to FAILED, which removes it from the claim loop. Every failure is audited
   * with message status, lease ownership and version moving together atomically.
   */
  fail(messageId: string, owner: string, expectedVersion: number, error: string, terminal: boolean, now: string): void {
    const message = this.require(messageId);
    if (message.status !== 'PENDING') return;
    this.requireHolder(message, owner, expectedVersion);
    message.lastError = error;
    message.leaseOwner = null;
    message.leaseUntil = null;
    message.version += 1;
    if (terminal) {
      message.status = 'FAILED';
      message.failedAt = now;
    }
    this.store.addAudit(message.tenantId, terminal ? 'outbox.failed' : 'outbox.retry-scheduled', 'outbox', message.id, {
      owner,
      version: message.version,
      attempts: message.attempts,
      terminal,
      error,
    });
  }

  private grant(message: OutboxMessage, owner: string, at: number): void {
    message.attempts += 1;
    message.leaseOwner = owner;
    message.leaseUntil = new Date(at + LEASE_TTL_MS).toISOString();
    message.version += 1;
    message.lastError = null;
  }

  private token(message: OutboxMessage): OutboxLease {
    return { messageId: message.id, owner: message.leaseOwner as string, leaseUntil: message.leaseUntil as string, version: message.version };
  }

  private isExpired(message: OutboxMessage, at: number): boolean {
    return message.leaseUntil === null || new Date(message.leaseUntil).getTime() <= at;
  }

  private require(messageId: string): OutboxMessage {
    const message = this.store.outbox.find((item) => item.id === messageId);
    if (!message) throw new Error(`outbox message ${messageId} not found`);
    return message;
  }

  private requireHolder(message: OutboxMessage, owner: string, expectedVersion: number): void {
    if (message.leaseOwner !== owner) throw new LeaseNotHeldError(message.id);
    if (message.version !== expectedVersion) throw new LeaseVersionConflictError(message.id, expectedVersion, message.version);
  }
}
