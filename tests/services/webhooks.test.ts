import fetch from 'node-fetch';
import crypto from 'crypto';
import { postWebhookWithRetry, signWebhookPayload, dispatchEventWebhook } from '../../src/services/webhooks';
import { createWebhookSubscription, listWebhookDeadLetters } from '../../src/db';
import { getVersionInfo } from '../../src/version';

jest.mock('node-fetch', () => jest.fn());

const mockedFetch = fetch as jest.MockedFunction<typeof fetch>;

function uniqueUrl(label: string): string {
  return `https://example.com/hook-${label}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

describe('postWebhookWithRetry', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('returns successfully when the first request succeeds', async () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    mockedFetch.mockResolvedValue({ ok: true, status: 200 } as any);

    await expect(postWebhookWithRetry('https://example.com', { eventType: 'test' })).resolves.toBeUndefined();
    expect(mockedFetch).toHaveBeenCalledTimes(1);
  });

  it('retries on an initial failure and succeeds on a later attempt', async () => {
    mockedFetch.mockRejectedValueOnce(new Error('network fail'));
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    mockedFetch.mockResolvedValue({ ok: true, status: 200 } as any);

    await expect(
      postWebhookWithRetry('https://example.com', { eventType: 'test' }, { retries: 3, baseDelayMs: 1, maxDelayMs: 2 })
    ).resolves.toBeUndefined();

    expect(mockedFetch).toHaveBeenCalledTimes(2);
  });

  it('throws after all retries fail', async () => {
    mockedFetch.mockRejectedValue(new Error('network down'));

    await expect(
      postWebhookWithRetry('https://example.com', { eventType: 'test' }, { retries: 2, baseDelayMs: 1, maxDelayMs: 2 })
    ).rejects.toThrow('network down');

    expect(mockedFetch).toHaveBeenCalledTimes(2);
  });

  it('signs the raw request body and attaches X-Webhook-Signature when a secret is provided', async () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    mockedFetch.mockResolvedValue({ ok: true, status: 200 } as any);
    const payload = { eventType: 'test', payload: { a: 1 } };

    await postWebhookWithRetry('https://example.com', payload, { secret: 'shh-secret' });

    expect(mockedFetch).toHaveBeenCalledTimes(1);
    const [, init] = mockedFetch.mock.calls[0];
    const rawBody = init!.body as string;
    expect(rawBody).toBe(JSON.stringify(payload));

    const signatureHeader = (init!.headers as Record<string, string>)['X-Webhook-Signature'];
    expect(signatureHeader).toMatch(/^sha256=[0-9a-f]{64}$/);

    const expectedDigest = crypto.createHmac('sha256', 'shh-secret').update(rawBody).digest('hex');
    expect(signatureHeader).toBe(`sha256=${expectedDigest}`);
  });

  it('omits the signature header when no secret is provided', async () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    mockedFetch.mockResolvedValue({ ok: true, status: 200 } as any);

    await postWebhookWithRetry('https://example.com', { eventType: 'test' });

    const [, init] = mockedFetch.mock.calls[0];
    expect((init!.headers as Record<string, string>)['X-Webhook-Signature']).toBeUndefined();
  });

  it('sends User-Agent, X-Webhook-Event and X-Webhook-Delivery headers', async () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    mockedFetch.mockResolvedValue({ ok: true, status: 200 } as any);

    await postWebhookWithRetry(
      'https://example.com',
      { eventType: 'player_registered', payload: { wallet: 'GABC' } },
      { eventType: 'player_registered', deliveryId: 'delivery-123' }
    );

    expect(mockedFetch).toHaveBeenCalledTimes(1);
    const [, init] = mockedFetch.mock.calls[0];
    const headers = init!.headers as Record<string, string>;
    expect(headers['User-Agent']).toBe(`ScoutOff-Webhooks/${getVersionInfo().version}`);
    expect(headers['X-Webhook-Event']).toBe('player_registered');
    expect(headers['X-Webhook-Delivery']).toBe('delivery-123');
  });

  it('sends identical headers on every retry attempt', async () => {
    mockedFetch.mockRejectedValueOnce(new Error('network fail'));
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    mockedFetch.mockResolvedValue({ ok: true, status: 200 } as any);

    await postWebhookWithRetry(
      'https://example.com',
      { eventType: 'milestone_approved', payload: { milestoneId: 'm1' } },
      { eventType: 'milestone_approved', deliveryId: 'delivery-456', retries: 3, baseDelayMs: 1, maxDelayMs: 2 }
    );

    expect(mockedFetch).toHaveBeenCalledTimes(2);
    for (const [, init] of mockedFetch.mock.calls) {
      const headers = init!.headers as Record<string, string>;
      expect(headers['User-Agent']).toBe(`ScoutOff-Webhooks/${getVersionInfo().version}`);
      expect(headers['X-Webhook-Event']).toBe('milestone_approved');
      expect(headers['X-Webhook-Delivery']).toBe('delivery-456');
    }
  });

  it(
    'fails within the configured timeout when the subscriber never responds',
    async () => {
      // Simulates a subscriber that accepts the TCP connection but never sends
      // a response: the underlying fetch promise never settles on its own.
      // A real `node-fetch` call passed an aborted signal rejects with an
      // AbortError, so we mimic that here to exercise our abort wiring
      // without depending on real network timing.
      mockedFetch.mockImplementation((_url, init) => {
        return new Promise((_resolve, reject) => {
          const signal = init?.signal as AbortSignal | undefined;
          signal?.addEventListener('abort', () => {
            const err = new Error('The operation was aborted');
            err.name = 'AbortError';
            reject(err);
          });
        }) as ReturnType<typeof fetch>;
      });

      const startedAt = Date.now();
      await expect(
        postWebhookWithRetry('https://example.com', { eventType: 'test' }, {
          retries: 1,
          timeoutMs: 50,
        })
      ).rejects.toThrow(/timed out/i);

      // The attempt must fail close to the configured timeout, not hang
      // indefinitely (well under the 10s test timeout below).
      expect(Date.now() - startedAt).toBeLessThan(2000);
      expect(mockedFetch).toHaveBeenCalledTimes(1);
      const [, init] = mockedFetch.mock.calls[0];
      expect(init!.signal).toBeDefined();
    },
    10000
  );
});

describe('signWebhookPayload', () => {
  it('produces the documented sha256=<hex> format, verifiable by recomputing the HMAC with the same secret', () => {
    const secret = 'my-subscriber-secret';
    const rawBody = JSON.stringify({ eventType: 'player_registered', payload: { wallet: 'GABC' } });

    const signature = signWebhookPayload(rawBody, secret);
    expect(signature).toMatch(/^sha256=[0-9a-f]{64}$/);

    // A receiver recomputing the HMAC over the same raw body with the same
    // secret must derive the identical signature (docs/webhooks.md).
    const recomputed = crypto.createHmac('sha256', secret).update(rawBody).digest('hex');
    expect(signature).toBe(`sha256=${recomputed}`);
  });

  it('produces a different signature for a different secret or a different body', () => {
    const rawBody = JSON.stringify({ eventType: 'test' });
    expect(signWebhookPayload(rawBody, 'secret-a')).not.toBe(signWebhookPayload(rawBody, 'secret-b'));

    const otherBody = JSON.stringify({ eventType: 'other' });
    expect(signWebhookPayload(rawBody, 'secret-a')).not.toBe(signWebhookPayload(otherBody, 'secret-a'));
  });
});

describe('dispatchEventWebhook', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('delivers to a registered subscription signed with its own secret', async () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    mockedFetch.mockResolvedValue({ ok: true, status: 200 } as any);
    const url = uniqueUrl('delivered');
    const secret = 'subscriber-secret-a';
    createWebhookSubscription(url, secret);

    await dispatchEventWebhook('player_registered', { wallet: 'GABC' });

    const call = mockedFetch.mock.calls.find(([calledUrl]) => calledUrl === url);
    expect(call).toBeDefined();
    const [, init] = call!;
    const rawBody = init!.body as string;
    const signatureHeader = (init!.headers as Record<string, string>)['X-Webhook-Signature'];
    expect(signatureHeader).toBe(signWebhookPayload(rawBody, secret));
    const parsed = JSON.parse(rawBody);
    expect(parsed.eventType).toBe('player_registered');
    expect(parsed.payload).toEqual({ wallet: 'GABC' });
    // Delivery ID must be present and covered by the HMAC signature
    expect(parsed.deliveryId).toBeDefined();
    expect(typeof parsed.deliveryId).toBe('string');
    expect(parsed.deliveryId.length).toBeGreaterThan(0);

    // Delivery headers must identify the sender, event and delivery
    const headers = init!.headers as Record<string, string>;
    expect(headers['User-Agent']).toBe(`ScoutOff-Webhooks/${getVersionInfo().version}`);
    expect(headers['X-Webhook-Event']).toBe('player_registered');
    expect(headers['X-Webhook-Delivery']).toBe(parsed.deliveryId);
  });

  it(
    'persists a dead letter with the right fields when retries are exhausted, without throwing',
    async () => {
      mockedFetch.mockRejectedValue(new Error('connection refused'));
      const url = uniqueUrl('dead-letter');
      const secret = 'subscriber-secret-b';
      const subscription = createWebhookSubscription(url, secret);

      await expect(dispatchEventWebhook('milestone_approved', { milestoneId: 'm1' })).resolves.toBeUndefined();

      const deadLetters = listWebhookDeadLetters(100, 0);
      const match = deadLetters.find((d) => d.url === url);
      expect(match).toBeDefined();
      expect(match!.subscription_id).toBe(subscription.id);
      expect(match!.event_type).toBe('milestone_approved');
      // Payload must include deliveryId
      const parsedPayload = JSON.parse(match!.payload);
      expect(parsedPayload.eventType).toBe('milestone_approved');
      expect(parsedPayload.payload).toEqual({ milestoneId: 'm1' });
      expect(parsedPayload.deliveryId).toBeDefined();
      expect(typeof parsedPayload.deliveryId).toBe('string');
      expect(parsedPayload.deliveryId.length).toBeGreaterThan(0);
    },
    10000
  );
});
