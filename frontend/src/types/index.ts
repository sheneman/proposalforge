export type PhaseStatusValue = 'idle' | 'pending' | 'running' | 'completed' | 'failed';

export interface PhaseStatus {
  phase: number;
  name: string;
  status: PhaseStatusValue;
  total: number;
  processed: number;
  errors: number;
  detail: string;
  error_log: string[];
}

export interface PipelineStatus {
  is_running: boolean;
  started_at: string | null;
  current_phase: number | null;
  config: { types: string[] };
  phases: PhaseStatus[];
}

export interface ModelSettings {
  base_url: string;
  model: string;
  api_key: string;
  [key: string]: string;
}

export interface OCRSettings extends ModelSettings {
  ocr_method: string;
  doc_workers: string;
}

export interface AppSettings {
  timezone: string;
}

export interface HealthStats {
  total: number;
  [key: string]: number | string;
}

export interface SyncLogEntry {
  id: number;
  sync_type: string;
  status: string;
  started_at: string;
  completed_at: string | null;
  duration_seconds: number | null;
  total_items: number;
  success_count: number;
  error_count: number;
  error_message: string | null;
}

export interface SchedulerConfig {
  enabled: boolean;
  interval_hours?: number;
  schedule_day?: string;
  schedule_hour?: number;
  schedule_minute?: number;
  next_run?: string;
}

export interface ResearcherSyncStatus {
  is_syncing: boolean;
  stats: Record<string, unknown>;
}

export interface MatchStatus {
  is_computing: boolean;
  stats: Record<string, unknown>;
}
