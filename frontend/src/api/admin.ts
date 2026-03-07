import { apiGet, apiPost } from './client';
import type {
  ModelSettings, OCRSettings, AppSettings, HealthStats,
  SyncLogEntry, SchedulerConfig, ResearcherSyncStatus, MatchStatus,
} from '../types';

// Settings
export const getSettings = (section: string) => apiGet<Record<string, string>>(`/settings/${section}`);
export const saveSettings = (section: string, data: Record<string, string>) =>
  apiPost<{ ok: boolean }>(`/settings/${section}`, data);
export const testSettings = (section: string, data: Record<string, string>) =>
  apiPost<{ ok: boolean; message?: string; error?: string }>(`/settings/${section}/test`, data);

// Health
export const getGrantsHealth = () => apiGet<HealthStats>('/health/grants');
export const getCollabNetHealth = () => apiGet<HealthStats>('/health/collabnet');

// Sync History
export const getSyncHistory = () => apiGet<SyncLogEntry[]>('/sync/history');

// Scheduler
export const getGrantsScheduler = () => apiGet<SchedulerConfig>('/scheduler/grants');
export const toggleGrantsScheduler = () => apiPost<{ ok: boolean }>('/scheduler/grants/toggle');
export const setGrantsInterval = (hours: number) =>
  apiPost<{ ok: boolean }>('/scheduler/grants/interval', { hours });
export const getCollabNetScheduler = () => apiGet<SchedulerConfig>('/scheduler/collabnet');
export const toggleCollabNetScheduler = () => apiPost<{ ok: boolean }>('/scheduler/collabnet/toggle');
export const setCollabNetSchedule = (data: { day: string; hour: number; minute: number }) =>
  apiPost<{ ok: boolean }>('/scheduler/collabnet/schedule', data);

// Researcher sync
export const getResearcherSyncStatus = () => apiGet<ResearcherSyncStatus>('/researcher-sync/status');
export const triggerResearcherSync = () => apiPost<{ ok: boolean }>('/researcher-sync/trigger');
export const cancelResearcherSync = () => apiPost<{ ok: boolean }>('/researcher-sync/cancel');

// Matches
export const getMatchStatus = () => apiGet<MatchStatus>('/matches/status');
export const recomputeMatches = () => apiPost<{ ok: boolean }>('/matches/recompute');

// Auth
export const getAuthStatus = () => apiGet<{ is_admin: boolean }>('/auth/status');
