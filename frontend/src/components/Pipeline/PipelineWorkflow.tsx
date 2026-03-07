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
