import { apiGet, apiPost } from './client';
import type { PipelineStatus } from '../types';

export function getPipelineStatus(): Promise<PipelineStatus> {
  return apiGet<PipelineStatus>('/pipeline/status');
}

export function startPipeline(types: string[]): Promise<{ ok: boolean }> {
  return apiPost<{ ok: boolean }>('/pipeline/start', { types });
}

export function cancelPipeline(): Promise<{ ok: boolean }> {
  return apiPost<{ ok: boolean }>('/pipeline/cancel');
}
