import { useState } from 'react';
import { Button, ProgressBar, Alert } from 'react-bootstrap';
import PhaseCard from './PhaseCard';
import TypeSelector from './TypeSelector';
import { startPipeline, cancelPipeline } from '../../api/pipeline';
import { usePipeline } from '../../hooks/usePipeline';

export default function PipelineWorkflow() {
  const { status, error, refresh } = usePipeline();
  const [selectedTypes, setSelectedTypes] = useState<string[]>(['posted', 'forecasted']);
  const [actionError, setActionError] = useState<string | null>(null);

  const handleStart = async () => {
    if (selectedTypes.length === 0) {
      setActionError('Select at least one opportunity type');
      return;
    }
    setActionError(null);
    try {
      await startPipeline(selectedTypes);
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : 'Failed to start pipeline');
    }
  };

  const handleCancel = async () => {
    setActionError(null);
    try {
      await cancelPipeline();
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : 'Failed to cancel pipeline');
    }
  };

  if (error) return <Alert variant="danger">{error}</Alert>;
  if (!status) return <div className="text-center py-4"><div className="spinner-border text-secondary" /></div>;

  const runningPhase = status.phases.find((p) => p.status === 'running');
  const overallProgress = runningPhase && runningPhase.total > 0
    ? Math.round((runningPhase.processed / runningPhase.total) * 100)
    : null;

  return (
    <div className="border rounded-3 p-3 bg-white">
      <div className="d-flex justify-content-between align-items-center mb-3">
        <h6 className="mb-0 fw-bold">
          <i className="bi bi-diagram-3 me-2 text-navy"></i>Grants.gov Pipeline
        </h6>
        {status.is_running && status.started_at && (
          <small className="text-muted">
            Started {new Date(status.started_at).toLocaleTimeString()}
          </small>
        )}
      </div>

      <TypeSelector
        selected={status.is_running ? (status.config.types || selectedTypes) : selectedTypes}
        onChange={setSelectedTypes}
        disabled={status.is_running}
      />

      <div className="d-flex align-items-center gap-2 my-3 flex-wrap justify-content-center">
        {status.phases.map((phase, idx) => (
          <div key={phase.phase} className="d-flex align-items-center">
            <PhaseCard phase={phase} />
            {idx < status.phases.length - 1 && (
              <i className="bi bi-chevron-right text-muted mx-1" style={{ fontSize: '1.2rem' }}></i>
            )}
          </div>
        ))}
      </div>

      {overallProgress !== null && (
        <div className="mb-2">
          <ProgressBar
            now={overallProgress}
            label={`${overallProgress}%`}
            variant="warning"
            animated
            style={{ height: 8 }}
          />
          {runningPhase && (
            <small className="text-muted">
              {runningPhase.name}: {runningPhase.processed.toLocaleString()}/{runningPhase.total.toLocaleString()}
              {runningPhase.detail && ` — ${runningPhase.detail}`}
            </small>
          )}
        </div>
      )}

      {/* Summary line showing detail for completed/failed phases */}
      {!status.is_running && (() => {
        const failedPhases = status.phases.filter(p => p.status === 'failed');
        const totalErrors = status.phases.reduce((s, p) => s + p.errors, 0);
        const lastCompleted = [...status.phases].reverse().find(p => p.status === 'completed' || p.status === 'failed');
        return (failedPhases.length > 0 || totalErrors > 0) ? (
          <Alert variant={failedPhases.length > 0 ? 'danger' : 'warning'} className="py-2 small mb-2">
            <div className="fw-semibold">
              {failedPhases.length > 0
                ? <><i className="bi bi-exclamation-triangle me-1"></i>Pipeline stopped: {failedPhases.map(p => p.name).join(', ')} failed</>
                : <><i className="bi bi-check-circle me-1"></i>Pipeline completed with {totalErrors} error(s)</>
              }
            </div>
            {status.phases.filter(p => p.detail).map((p) => (
              <div key={p.phase} className="text-muted" style={{ fontSize: '0.75rem' }}>
                {p.name}: {p.detail}
              </div>
            ))}
            <div className="mt-1" style={{ fontSize: '0.7rem' }}>
              <i className="bi bi-info-circle me-1"></i>Click any phase with errors for details
            </div>
          </Alert>
        ) : lastCompleted ? (
          <div className="text-center text-success small mb-2">
            <i className="bi bi-check-circle me-1"></i>Pipeline completed successfully
          </div>
        ) : null;
      })()}

      {actionError && <Alert variant="danger" className="py-1 small">{actionError}</Alert>}

      <div className="d-flex gap-2 mt-2">
        <Button
          variant="primary"
          size="sm"
          className="btn-navy"
          onClick={handleStart}
          disabled={status.is_running}
        >
          <i className="bi bi-play-fill me-1"></i>Run Pipeline
        </Button>
        <Button
          variant="outline-danger"
          size="sm"
          onClick={handleCancel}
          disabled={!status.is_running}
        >
          <i className="bi bi-stop-fill me-1"></i>Cancel
        </Button>
      </div>
    </div>
  );
}
