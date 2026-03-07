import type { PhaseStatus, PhaseStatusValue } from '../../types';

const ICONS: Record<number, string> = {
  1: 'bi-cloud-download',
  2: 'bi-file-earmark-arrow-down',
  3: 'bi-link-45deg',
  4: 'bi-file-earmark-text',
  5: 'bi-tags',
  6: 'bi-vector-pen',
};

const COUNT_LABELS: Record<number, string> = {
  1: 'found',
  2: 'fetched',
  3: 'retrieved',
  4: 'extracted',
  5: 'classified',
  6: 'embedded',
};

const STATUS_STYLES: Record<PhaseStatusValue, { border: string; badge: string; badgeIcon: string }> = {
  idle: { border: 'border-secondary opacity-50', badge: '', badgeIcon: '' },
  pending: { border: 'border-secondary', badge: '', badgeIcon: '' },
  running: { border: 'border-warning pipeline-active-border', badge: 'bg-warning text-dark', badgeIcon: 'bi-arrow-repeat spinning' },
  completed: { border: 'border-success', badge: 'bg-success', badgeIcon: 'bi-check-lg' },
  failed: { border: 'border-danger', badge: 'bg-danger', badgeIcon: 'bi-exclamation-triangle' },
};

interface Props {
  phase: PhaseStatus;
}

export default function PhaseCard({ phase }: Props) {
  const style = STATUS_STYLES[phase.status];
  const icon = ICONS[phase.phase] || 'bi-circle';
  const countLabel = COUNT_LABELS[phase.phase] || '';

  return (
    <div className={`pipeline-phase-card border rounded-3 p-3 text-center position-relative ${style.border}`}
         style={{ minWidth: 110, maxWidth: 130 }}>
      {style.badge && (
        <span className={`position-absolute top-0 end-0 translate-middle badge rounded-pill ${style.badge}`}
              style={{ fontSize: '0.65rem' }}>
          <i className={style.badgeIcon}></i>
        </span>
      )}
      <div className="mb-1">
        <i className={`bi ${icon}${phase.status === 'running' ? ' phase-icon-pulse text-warning' : ''}`} style={{ fontSize: '1.5rem' }}></i>
      </div>
      <div className="fw-semibold small">{phase.name}</div>
      {(phase.status !== 'idle' && phase.status !== 'pending') && (
        <>
          <div className="fw-bold" style={{ fontSize: '1.1rem' }}>
            {phase.processed.toLocaleString()}
          </div>
          <div className="text-muted" style={{ fontSize: '0.7rem' }}>{countLabel}</div>
        </>
      )}
      {phase.status === 'running' && phase.total > 0 && (
        <div className="text-muted" style={{ fontSize: '0.65rem' }}>
          {phase.processed}/{phase.total}
        </div>
      )}
      {phase.errors > 0 && (
        <div className="text-danger" style={{ fontSize: '0.65rem' }}>
          {phase.errors} errors
        </div>
      )}
    </div>
  );
}
