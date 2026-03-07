import { useState, useEffect, useCallback, useRef } from 'react';
import { getPipelineStatus } from '../api/pipeline';
import type { PipelineStatus } from '../types';

const POLL_ACTIVE = 3000;
const POLL_IDLE = 30000;

export function usePipeline() {
  const [status, setStatus] = useState<PipelineStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const refresh = useCallback(async () => {
    try {
      const data = await getPipelineStatus();
      setStatus(data);
      setError(null);
      return data;
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch pipeline status');
      return null;
    }
  }, []);

  useEffect(() => {
    let mounted = true;

    async function poll() {
      if (!mounted) return;
      const data = await refresh();
      if (!mounted) return;
      const interval = data?.is_running ? POLL_ACTIVE : POLL_IDLE;
      timerRef.current = setTimeout(poll, interval);
    }

    poll();

    return () => {
      mounted = false;
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [refresh]);

  return { status, error, refresh };
}
