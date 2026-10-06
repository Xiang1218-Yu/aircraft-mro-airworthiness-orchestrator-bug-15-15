import type { OutboxConfirmResult, OutboxLease, OutboxMessage } from '../domain/types.js';
import { Store } from '../store/store.js';

const LEASE_TTL_MS = 30_000;
const MAX_ATTEMPTS = 5;

/**
 * Outbox lease coordinator.
 *
 * Invariant: a PENDING outbox message is publishable by exactly one owner at a
 * time. The current holder is identified by (leaseOwner, version); every claim,
 * renewal and state transition bumps `version`, so a stale owner can be detected
 * even if it is still holding an earlier lease snapshot. All checks and the
 * corresponding message/audit mutations run under the store-wide mutex so that
 * claim/renew/confirm/fail interleave atomically.
 *
 * The outbox message id is the publish identity: downstream consumers must use
 * it (e.g. as the broker/event id) for idempotent application of business
 * effects, mirroring the de-duplication in WorkOrderService/ExternalEventService.
 */
export class OutboxLeaseService {
  constructor(private readonly store: Store) {}

  /**
   * Claim every message that is due: PENDING, below the attempt cap and either
   * never leased or whose lease has expired. A message with an unexpired lease
   * is never handed to another consumer.
   */
  async claim(owner: string, now: string): Promise<OutboxLease[]> {
    return this.store.lock.runExclusive(() => {
      const claimed: OutboxLease[] = [];
      for (const message of this.store.outbox) {
        if (message.status !== 'PENDING' || message.attempts >= MAX_ATTEMPTS) continue;
        if (this.isHeld(message, now)) continue;
        this.grant(message, owner, now);
        claimed.push(this.snapshot(message));
      }
      return claimed;
    });
  }

  /**
   * Extend the lease. Only the current holder of the exact lease version it was
   * granted (and last renewed) may renew, and only while that lease is still
   * valid. A timed-out lease that was re-granted to another consumer cannot be
   * resurrected by the previous owner.
   */
  async renew(messageId: string, owner: string, expectedVersion: number, now: string): Promise<OutboxLease> {
    return this.store.lock.runExclusive(() => {
      const message = this.requirePending(messageId);
      if (!message.leaseOwner || message.leaseOwner !== owner) throw new Error('lease not held by owner');
      if (message.version !== expectedVersion) throw new Error('lease version conflict');
      if (!this.isHeld(message, now)) throw new Error('lease has expired');
      this.grant(message, owner, now);
      return this.snapshot(message);
    });
  }

  /**
   * Acknowledge a successful publish and transition PENDING -> PUBLISHED.
   *
   * The transition is a compare-and-set on (status, owner, version): the
   * (owner, version) pair is the fencing token handed out by claim/renew, and
   * every re-claim after a lease expiry mints a new version that invalidates
   * the previous owner's token. At most one confirm therefore succeeds per
   * message, even after a lease expiry/re-claim, and exactly one audit record
   * is written. A repeat confirm from the same owner (e.g. after a network
   * retry that lost the first response) is reported as ALREADY_PUBLISHED
   * without a second audit or business effect; a stale owner whose token was
   * invalidated by re-claim is rejected.
   *
   * Note: an expired-but-superseded token is rejected via version mismatch
   * (another consumer re-claimed); an expired token that nobody has replaced
   * still confirms, which avoids a needless duplicate publish when the original
   * owner finished just after the timeout. Brokers/downstream must still
   * de-duplicate on the outbox message id for the rare double-delivery case
   * where two owners both physically published around the timeout boundary.
   */
  async confirm(messageId: string, owner: string, expectedVersion: number): Promise<OutboxConfirmResult> {
    return this.store.lock.runExclusive(() => {
      const message = this.store.outbox.find((item) => item.id === messageId);
      if (!message) throw new Error('outbox message not found');

      if (message.status === 'PUBLISHED') {
        if (message.publishedBy === owner) return { outcome: 'ALREADY_PUBLISHED', messageId, version: message.version };
        throw new Error('message already published by another owner');
      }
      if (message.status === 'FAILED') throw new Error('message has failed permanently');

      this.assertHolds(message, owner, expectedVersion);
      message.status = 'PUBLISHED';
      message.publishedBy = owner;
      message.version += 1;
      this.clearLease(message);
      this.store.addAudit(message.tenantId, 'outbox.published', 'outbox', message.id, { owner, attempts: message.attempts });
      return { outcome: 'PUBLISHED', messageId, version: message.version };
    });
  }

  /**
   * Record a failed publish attempt. Only the holder of the current fencing
   * token (owner, version) may report a failure, so a timed-out worker whose
   * lease was re-granted (and whose version was therefore invalidated) cannot
   * mutate a message another consumer is now handling. The lease is released;
   * the message stays PENDING (and becomes re-claimable) until the attempt cap
   * is reached, after which it transitions to FAILED.
   */
  async fail(messageId: string, owner: string, expectedVersion: number, reason: string): Promise<'RETRY' | 'FAILED'> {
    return this.store.lock.runExclusive(() => {
      const message = this.requirePending(messageId);
      this.assertHolds(message, owner, expectedVersion);
      message.attempts += 1;
      message.version += 1;
      this.clearLease(message);
      const retryable = message.attempts < MAX_ATTEMPTS;
      if (!retryable) message.status = 'FAILED';
      this.store.addAudit(message.tenantId, retryable ? 'outbox.publish-failed' : 'outbox.dead-lettered', 'outbox', message.id, {
        owner,
        reason,
        attempts: message.attempts,
        retryable,
      });
      return retryable ? 'RETRY' : 'FAILED';
    });
  }

  /** Put a dead-lettered message back into rotation (manual intervention / retry). */
  async requeue(messageId: string): Promise<void> {
    await this.store.lock.runExclusive(() => {
      const message = this.store.outbox.find((item) => item.id === messageId);
      if (!message) throw new Error('outbox message not found');
      if (message.status !== 'FAILED') throw new Error('only failed messages can be requeued');
      message.status = 'PENDING';
      message.attempts = 0;
      message.version += 1;
      this.clearLease(message);
      this.store.addAudit(message.tenantId, 'outbox.requeued', 'outbox', message.id, {});
    });
  }

  private requirePending(messageId: string): OutboxMessage {
    const message = this.store.outbox.find((item) => item.id === messageId);
    if (!message) throw new Error('outbox message not found');
    if (message.status !== 'PENDING') throw new Error('message is not pending');
    return message;
  }

  private assertHolds(message: OutboxMessage, owner: string, expectedVersion: number): void {
    if (!message.leaseOwner || message.leaseOwner !== owner) throw new Error('lease not held by owner');
    if (message.version !== expectedVersion) throw new Error('lease version conflict');
  }

  private isHeld(message: OutboxMessage, now: string): boolean {
    return Boolean(message.leaseOwner && message.leaseUntil && new Date(message.leaseUntil).getTime() > new Date(now).getTime());
  }

  private grant(message: OutboxMessage, owner: string, now: string): void {
    message.leaseOwner = owner;
    message.leaseUntil = new Date(new Date(now).getTime() + LEASE_TTL_MS).toISOString();
    message.version += 1;
  }

  private clearLease(message: OutboxMessage): void {
    delete message.leaseOwner;
    delete message.leaseUntil;
  }

  private snapshot(message: OutboxMessage): OutboxLease {
    return {
      messageId: message.id,
      owner: message.leaseOwner as string,
      leaseUntil: message.leaseUntil as string,
      version: message.version,
    };
  }
}
