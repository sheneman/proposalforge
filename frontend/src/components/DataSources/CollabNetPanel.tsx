import { useState } from 'react';
import { Card, Button, Spinner, Row, Col, Alert } from 'react-bootstrap';
import { useApi } from '../../hooks/useApi';
import {
  getCollabNetHealth, getResearcherSyncStatus,
  triggerResearcherSync, cancelResearcherSync,
  getMatchStatus, recomputeMatches,
} from '../../api/admin';

export default function CollabNetPanel() {
  const { data: health, loading: healthLoading } = useApi(getCollabNetHealth);
  const { data: syncStatus, refresh: refreshSync } = useApi(getResearcherSyncStatus);
  const { data: matchStatus, refresh: refreshMatch } = useApi(getMatchStatus);
  const [error, setError] = useState<string | null>(null);

  const handleSync = async () => {
    setError(null);
    try {
      await triggerResearcherSync();
      await refreshSync();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed');
    }
  };

  const handleCancelSync = async () => {
    try {
      await cancelResearcherSync();
      await refreshSync();
    } catch {
      /* ignore */
    }
  };

  const handleRecomputeMatches = async () => {
    setError(null);
    try {
      await recomputeMatches();
      await refreshMatch();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed');
    }
  };

  return (
    <div className="border rounded-3 p-3 bg-white">
      <h6 className="mb-3 fw-bold">
        <i className="bi bi-people me-2 text-navy"></i>CollabNet Researchers
      </h6>

      {error && <Alert variant="danger" className="py-1 small">{error}</Alert>}

      <div className="d-flex gap-2 mb-3">
        <Button size="sm" className="btn-navy" onClick={handleSync}
                disabled={syncStatus?.is_syncing}>
          {syncStatus?.is_syncing
            ? <><Spinner size="sm" className="me-1" />Syncing...</>
            : <><i className="bi bi-arrow-repeat me-1"></i>Sync Researchers</>}
        </Button>
        {syncStatus?.is_syncing && (
          <Button size="sm" variant="outline-danger" onClick={handleCancelSync}>Cancel</Button>
        )}
        <Button size="sm" variant="outline-primary" onClick={handleRecomputeMatches}
                disabled={matchStatus?.is_computing}>
          {matchStatus?.is_computing
            ? <><Spinner size="sm" className="me-1" />Computing...</>
            : <><i className="bi bi-arrow-left-right me-1"></i>Recompute Matches</>}
        </Button>
      </div>

      <h6 className="fw-bold small">
        <i className="bi bi-heart-pulse me-2"></i>Data Health
      </h6>
      {healthLoading ? (
        <Spinner size="sm" />
      ) : health ? (
        <Row className="g-2">
          {Object.entries(health).map(([key, val]) => (
            <Col xs={6} md={3} key={key}>
              <Card className="text-center p-2 h-100">
                <div className="fw-bold">{typeof val === 'number' ? val.toLocaleString() : val}</div>
                <small className="text-muted text-capitalize">{key.replace(/_/g, ' ')}</small>
              </Card>
            </Col>
          ))}
        </Row>
      ) : null}
    </div>
  );
}
