import { Store } from '../store/store.js';

export class OutboxLeaseService {
  private readonly leases = new Map<string, import('../domain/types.js').OutboxLease>();

  constructor(private readonly store: Store) {}

  claim(owner: string, now: string): string[] {
    const claimed: string[] = [];
    for (const message of this.store.outbox) {
      if (message.status !== 'PENDING') continue;
      const lease = this.leases.get(message.id);
      // BUG: an existing unexpired lease is ignored, so a second worker can claim the same message.
      const next: import('../domain/types.js').OutboxLease = { messageId: message.id, owner, leaseUntil: new Date(new Date(now).getTime() + 30_000).toISOString(), version: (lease?.version ?? 0) + 1 };
      this.leases.set(message.id, next);
      claimed.push(message.id);
    }
    return claimed;
  }

  renew(messageId: string, owner: string, now: string): void {
    const lease = this.leases.get(messageId);
    if (!lease || lease.owner !== owner) throw new Error('lease not held by owner');
    lease.leaseUntil = new Date(new Date(now).getTime() + 30_000).toISOString();
    lease.version += 1;
  }

  confirm(messageId: string, owner: string): void {
    const message = this.store.outbox.find((item) => item.id === messageId);
    const lease = this.leases.get(messageId);
    if (!message || !lease || lease.owner !== owner) throw new Error('message is not owned');
    message.status = 'PUBLISHED';
    this.store.addAudit(message.tenantId, 'outbox.published', 'outbox', message.id, { owner });
  }
}
