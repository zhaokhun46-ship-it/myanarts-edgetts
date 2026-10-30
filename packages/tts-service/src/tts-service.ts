import {
  createAbortError,
  TtsError,
  type SynthesisRequest,
  type SynthesisResult,
  type TtsProvider,
  type TtsVoice,
  type WordBoundary,
} from "@edgetts/tts-core";
import { type Permit, SynthesisLimiter } from "./synthesis-limiter.js";
import { segmentText } from "./text-segmenter.js";
import { VoiceCache } from "./voice-cache.js";

export const DEFAULT_MAX_CONCURRENT_SYNTHESES = 4;
export const DEFAULT_MAX_QUEUED_SYNTHESES = 16;

export interface TtsServiceOptions {
  readonly voiceCacheTtlMs?: number;
  readonly voiceCacheErrorBackoffMs?: number;
  readonly now?: () => number;
  readonly maxConcurrentSyntheses?: number;
  readonly maxQueuedSyntheses?: number;
}

export interface ListVoicesOptions {
  readonly forceRefresh?: boolean;
}

export interface SegmentedSynthesisOptions {
  readonly maxSegmentCodePoints: number;
  readonly includeWordBoundaries?: boolean;
}

export interface SegmentedSynthesisResult extends SynthesisResult {
  readonly segmentCount: number;
  readonly wordBoundaries?: Promise<readonly WordBoundary[]>;
}

/** Point-in-time counters for monitoring; totals are cumulative since the service started. */
export interface TtsServiceStats {
  readonly activeSyntheses: number;
  readonly queuedSyntheses: number;
  readonly rejectedSyntheses: {
    readonly queueFull: number;
    readonly queueTimeout: number;
    readonly unknownVoice: number;
  };
  /** Null until the voice catalog has been fetched successfully once. */
  readonly voiceCatalog: { readonly voices: number; readonly ageMs: number } | null;
}

class ManagedAudioStream implements AsyncIterableIterator<Uint8Array> {
  private readonly upstreamIterator: AsyncIterator<Uint8Array>;
  private readonly signal: AbortSignal;
  private readonly onAbort: () => void;
  private readonly permit: Permit;
  private released = false;

  constructor(upstreamAudio: AsyncIterable<Uint8Array>, signal: AbortSignal, permit: Permit) {
    this.signal = signal;
    this.permit = permit;
    this.upstreamIterator = upstreamAudio[Symbol.asyncIterator]();

    this.onAbort = () => {
      this.releaseOnce();
      void this.upstreamIterator.return?.().catch(() => {});
    };

    if (signal.aborted) {
      this.onAbort();
    } else {
      signal.addEventListener("abort", this.onAbort, { once: true });
    }
  }

  private releaseOnce(): void {
    if (this.released) {
      return;
    }
    this.released = true;
    this.signal.removeEventListener("abort", this.onAbort);
    this.permit.release();
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    return this;
  }

  async next(): Promise<IteratorResult<Uint8Array>> {
    if (this.signal.aborted) {
      throw createAbortError(this.signal.reason);
    }
    if (this.released) {
      return { done: true, value: undefined };
    }

    try {
      const result = await this.upstreamIterator.next();
      if (result.done) {
        this.releaseOnce();
        return { done: true, value: undefined };
      }
      return result;
    } catch (error) {
      this.releaseOnce();
      try {
        await this.upstreamIterator.return?.();
      } catch {
        // ignore secondary error on cleanup
      }
      throw error;
    }
  }

  async return(value?: unknown): Promise<IteratorResult<Uint8Array>> {
    this.releaseOnce();
    try {
      if (this.upstreamIterator.return) {
        return await this.upstreamIterator.return(value);
      }
    } catch {
      // ignore
    }
    return { done: true, value: value as undefined };
  }

  async throw(error?: unknown): Promise<IteratorResult<Uint8Array>> {
    this.releaseOnce();
    try {
      if (this.upstreamIterator.throw) {
        return await this.upstreamIterator.throw(error);
      }
    } catch {
      // ignore
    }
    throw error;
  }
}

/** Synthesizes the segments of one request; close() releases any upstream connection. */
interface SegmentSynthesizer {
  synthesize(text: string): Promise<SynthesisResult>;
  close(): void;
}

/**
 * Uses one provider session for every segment when the provider supports sessions, saving a
 * connection handshake per segment; otherwise asks the provider once per segment.
 */
async function openSegmentSynthesizer(
  provider: TtsProvider,
  request: SynthesisRequest,
  signal: AbortSignal,
): Promise<SegmentSynthesizer> {
  if (!provider.openSession) {
    return {
      synthesize: (text) => provider.synthesize({ ...request, text }, signal),
      close: () => {},
    };
  }
  const session = await provider.openSession(
    {
      voice: request.voice,
      ...(request.format !== undefined ? { format: request.format } : {}),
      ...(request.includeWordBoundaries !== undefined ? { includeWordBoundaries: request.includeWordBoundaries } : {}),
    },
    signal,
  );
  const requestOptions = {
    ...(request.prosody !== undefined ? { prosody: request.prosody } : {}),
    ...(request.includeWordBoundaries !== undefined ? { includeWordBoundaries: request.includeWordBoundaries } : {}),
  };
  return {
    synthesize: (text) => session.synthesize({ text, ...requestOptions }, signal),
    close: () => session.close(),
  };
}

class SegmentedAudioStream implements AsyncIterableIterator<Uint8Array> {
  private readonly synthesizer: SegmentSynthesizer;
  private readonly segments: readonly string[];
  private readonly signal: AbortSignal;
  private readonly permit: Permit;
  private readonly expectedFormat: string;
  private readonly expectedContentType: string;
  private currentSegmentIndex = 0;
  private currentIterator: AsyncIterator<Uint8Array> | null;
  private closed = false;
  private released = false;
  private readonly onAbort: () => void;
  private currentSynthesisResult: SynthesisResult;
  private currentAudioBytes = 0;
  private cumulativeAudioBytes = 0;
  private readonly collectedWordBoundaries: WordBoundary[] = [];
  readonly wordBoundaries: Promise<readonly WordBoundary[]>;
  private resolveWordBoundaries!: (boundaries: readonly WordBoundary[]) => void;

  constructor(
    synthesizer: SegmentSynthesizer,
    firstResult: SynthesisResult,
    segments: readonly string[],
    signal: AbortSignal,
    permit: Permit,
  ) {
    this.synthesizer = synthesizer;
    this.segments = segments;
    this.signal = signal;
    this.permit = permit;
    this.expectedFormat = firstResult.format;
    this.expectedContentType = firstResult.contentType;
    this.currentSynthesisResult = firstResult;
    this.currentIterator = firstResult.audio[Symbol.asyncIterator]();
    this.wordBoundaries = new Promise((resolve) => {
      this.resolveWordBoundaries = resolve;
    });

    this.onAbort = () => {
      this.closed = true;
      this.releaseOnce();
      void this.cleanupCurrent();
      this.resolveWordBoundaries(this.collectedWordBoundaries);
    };

    if (signal.aborted) {
      this.closed = true;
      this.releaseOnce();
      void this.cleanupCurrent();
    } else {
      signal.addEventListener("abort", this.onAbort, { once: true });
    }
  }

  private releaseOnce(): void {
    if (this.released) {
      return;
    }
    this.released = true;
    this.signal.removeEventListener("abort", this.onAbort);
    this.synthesizer.close();
    this.permit.release();
  }

  private async cleanupCurrent(): Promise<void> {
    const iter = this.currentIterator;
    this.currentIterator = null;
    if (iter?.return) {
      try {
        await iter.return();
      } catch {
        // ignore
      }
    }
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    return this;
  }

  async next(): Promise<IteratorResult<Uint8Array>> {
    if (this.closed) {
      if (this.signal.aborted) {
        throw createAbortError(this.signal.reason);
      }
      return { done: true, value: undefined };
    }

    if (this.signal.aborted) {
      this.closed = true;
      this.releaseOnce();
      await this.cleanupCurrent();
      throw createAbortError(this.signal.reason);
    }

    while (this.currentSegmentIndex < this.segments.length) {
      if (!this.currentIterator) {
        if (this.signal.aborted) {
          this.closed = true;
          this.releaseOnce();
          throw createAbortError(this.signal.reason);
        }

        let nextResult: SynthesisResult;
        try {
          nextResult = await this.synthesizer.synthesize(this.segments[this.currentSegmentIndex]!);
        } catch (error) {
          this.closed = true;
          this.releaseOnce();
          await this.cleanupCurrent();
          throw error;
        }

        if (this.closed) {
          this.currentIterator = nextResult.audio[Symbol.asyncIterator]();
          await this.cleanupCurrent();
          if (this.signal.aborted) throw createAbortError(this.signal.reason);
          return { done: true, value: undefined };
        }

        if (
          nextResult.format !== this.expectedFormat ||
          nextResult.contentType !== this.expectedContentType
        ) {
          this.closed = true;
          this.releaseOnce();
          const iter = nextResult.audio[Symbol.asyncIterator]();
          await iter.return?.();
          throw new Error(
            `Segment ${this.currentSegmentIndex} returned inconsistent audio metadata: expected format ${this.expectedFormat}, got ${nextResult.format}; expected contentType ${this.expectedContentType}, got ${nextResult.contentType}`,
          );
        }

        this.currentIterator = nextResult.audio[Symbol.asyncIterator]();
        this.currentSynthesisResult = nextResult;
        this.currentAudioBytes = 0;
      }

      try {
        if (this.signal.aborted) {
          this.closed = true;
          this.releaseOnce();
          await this.cleanupCurrent();
          throw createAbortError(this.signal.reason);
        }

        const item = await this.currentIterator.next();
        if (this.closed) {
          if (this.signal.aborted) throw createAbortError(this.signal.reason);
          return { done: true, value: undefined };
        }
        if (!item.done) {
          this.currentAudioBytes += item.value.byteLength;
          return item;
        }

        // Current segment is done; advance to next segment
        const bitrate = this.expectedFormat === "mp3-96k" ? 96_000 : 48_000;
        const segmentBaseTicks = Math.floor(this.cumulativeAudioBytes * 8 * 10_000_000 / bitrate);
        const segmentBoundaries = await this.currentSynthesisResult.wordBoundaries?.catch(() => []);
        if (segmentBoundaries) {
          this.collectedWordBoundaries.push(...segmentBoundaries.map((boundary) => ({
            ...boundary,
            offset: segmentBaseTicks + boundary.offset,
          })));
        }
        this.cumulativeAudioBytes += this.currentAudioBytes;
        this.currentIterator = null;
        this.currentSegmentIndex++;
      } catch (error) {
        this.closed = true;
        this.releaseOnce();
        await this.cleanupCurrent();
        throw error;
      }
    }

    this.closed = true;
    this.releaseOnce();
    await this.cleanupCurrent();
    this.resolveWordBoundaries(this.collectedWordBoundaries);
    return { done: true, value: undefined };
  }

  async return(value?: unknown): Promise<IteratorResult<Uint8Array>> {
    this.closed = true;
    this.releaseOnce();
    await this.cleanupCurrent();
    this.resolveWordBoundaries(this.collectedWordBoundaries);
    return { done: true, value: value as undefined };
  }

  async throw(error?: unknown): Promise<IteratorResult<Uint8Array>> {
    this.closed = true;
    this.releaseOnce();
    await this.cleanupCurrent();
    this.resolveWordBoundaries(this.collectedWordBoundaries);
    throw error;
  }
}

export class TtsService {
  private readonly provider: TtsProvider;
  private readonly voiceCache: VoiceCache;
  private readonly limiter: SynthesisLimiter;
  private unknownVoiceRejections = 0;

  constructor(provider: TtsProvider, options?: TtsServiceOptions) {
    this.provider = provider;
    this.voiceCache = new VoiceCache(provider, {
      ...(options?.voiceCacheTtlMs !== undefined ? { ttlMs: options.voiceCacheTtlMs } : {}),
      ...(options?.voiceCacheErrorBackoffMs !== undefined
        ? { errorBackoffMs: options.voiceCacheErrorBackoffMs }
        : {}),
      ...(options?.now !== undefined ? { now: options.now } : {}),
    });
    this.limiter = new SynthesisLimiter({
      maxConcurrent: options?.maxConcurrentSyntheses ?? DEFAULT_MAX_CONCURRENT_SYNTHESES,
      maxQueued: options?.maxQueuedSyntheses ?? DEFAULT_MAX_QUEUED_SYNTHESES,
    });
  }

  getStats(): TtsServiceStats {
    return {
      activeSyntheses: this.limiter.active,
      queuedSyntheses: this.limiter.queued,
      rejectedSyntheses: { ...this.limiter.rejections, unknownVoice: this.unknownVoiceRejections },
      voiceCatalog: this.voiceCache.snapshotInfo(),
    };
  }

  async listVoices(options?: ListVoicesOptions): Promise<readonly TtsVoice[]> {
    return this.voiceCache.getVoices(options?.forceRefresh ?? false);
  }

  async synthesize(request: SynthesisRequest, signal: AbortSignal): Promise<SynthesisResult> {
    this.assertKnownVoice(request.voice);
    const permit = await this.limiter.acquire(signal);

    if (signal.aborted) {
      permit.release();
      throw createAbortError(signal.reason);
    }

    let rawResult: SynthesisResult;
    try {
      rawResult = await this.provider.synthesize(request, signal);
    } catch (error) {
      permit.release();
      throw error;
    }

    const result = this.wrapSynthesisResult(rawResult, signal, permit);
    if (signal.aborted) {
      throw createAbortError(signal.reason);
    }
    return result;
  }

  async synthesizeSegmented(
    request: SynthesisRequest,
    signal: AbortSignal,
    options: SegmentedSynthesisOptions,
  ): Promise<SegmentedSynthesisResult> {
    if (request.format === "webm-opus") {
      throw new RangeError("Segmented synthesis does not support webm-opus format");
    }

    if (
      options === null ||
      typeof options !== "object" ||
      typeof options.maxSegmentCodePoints !== "number" ||
      !Number.isInteger(options.maxSegmentCodePoints) ||
      options.maxSegmentCodePoints < 1
    ) {
      throw new RangeError("maxSegmentCodePoints must be a finite integer greater than 0");
    }

    if (signal.aborted) {
      throw createAbortError(signal.reason);
    }

    const segments =
      request.text.length === 0
        ? [request.text]
        : segmentText(request.text, { maxCodePoints: options.maxSegmentCodePoints });
    const segmentsToSynthesize = (segments.length === 0 ? [request.text] : segments).filter(
      (segment) => segment.trim().length > 0,
    );
    if (segmentsToSynthesize.length === 0) {
      throw new Error("Text must not be empty");
    }

    this.assertKnownVoice(request.voice);
    const permit = await this.limiter.acquire(signal);

    if (signal.aborted) {
      permit.release();
      throw createAbortError(signal.reason);
    }

    let synthesizer: SegmentSynthesizer;
    try {
      synthesizer = await openSegmentSynthesizer(this.provider, request, signal);
    } catch (error) {
      permit.release();
      throw error;
    }

    let firstResult: SynthesisResult;
    try {
      firstResult = await synthesizer.synthesize(segmentsToSynthesize[0]!);
    } catch (error) {
      synthesizer.close();
      permit.release();
      throw error;
    }

    const audioStream = new SegmentedAudioStream(
      synthesizer,
      firstResult,
      segmentsToSynthesize,
      signal,
      permit,
    );
    return {
      format: firstResult.format,
      contentType: firstResult.contentType,
      segmentCount: segmentsToSynthesize.length,
      audio: audioStream,
      ...(options.includeWordBoundaries ? { wordBoundaries: audioStream.wordBoundaries } : {}),
    };
  }

  /**
   * Rejects a voice missing from a fresh cached catalog before any capacity is used.
   * Never fetches: without a fresh catalog the request proceeds and the provider decides.
   * Matching ignores case so nothing the provider might accept is rejected here.
   */
  private assertKnownVoice(voice: string): void {
    const catalog = this.voiceCache.peekFresh();
    if (catalog === null || catalog.length === 0) {
      return;
    }
    const wanted = voice.toLowerCase();
    if (!catalog.some((candidate) => candidate.id.toLowerCase() === wanted)) {
      this.unknownVoiceRejections++;
      throw new TtsError("unknown_voice", "Requested voice is not in the voice catalog");
    }
  }

  private wrapSynthesisResult(
    result: SynthesisResult,
    signal: AbortSignal,
    permit: Permit,
  ): SynthesisResult {
    return {
      format: result.format,
      contentType: result.contentType,
      audio: new ManagedAudioStream(result.audio, signal, permit),
    };
  }
}
