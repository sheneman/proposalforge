import { useState } from 'react';
import { Form, Button, Badge, Alert, Spinner } from 'react-bootstrap';
import { useApi } from '../../hooks/useApi';
import { getGrantsScheduler, toggleGrantsScheduler, setGrantsInterval } from '../../api/admin';

const INTERVAL_OPTIONS = [1, 3, 6, 12, 24];

export default function GrantsScheduler() {
  const { data, loading, refresh } = useApi(getGrantsScheduler);
  const [error, setError] = useState<string | null>(null);

  const handleToggle = async () => {
    setError(null);
    try {
      await toggleGrantsScheduler();
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed');
    }
  };

  const handleInterval = async (hours: number) => {
    setError(null);
    try {
      await setGrantsInterval(hours);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed');
    }
  };

  if (loading) return <div className="text-center py-3"><Spinner size="sm" /></div>;

  return (
    <div className="border rounded-3 p-3 bg-white mb-3">
      <h6 className="fw-bold mb-3">
        <i className="bi bi-calendar-event me-2 text-navy"></i>Grants.gov Scheduler
      </h6>

      <div className="d-flex align-items-center gap-2 mb-3">
        <Button size="sm" variant={data?.enabled ? 'success' : 'outline-secondary'} onClick={handleToggle}>
          {data?.enabled ? 'Enabled' : 'Disabled'}
        </Button>
        {data?.next_run && (
          <small className="text-muted">
            Next: {new Date(data.next_run).toLocaleString()}
          </small>
        )}
      </div>

      <Form.Group>
        <Form.Label className="small fw-semibold">Sync every</Form.Label>
        <div className="d-flex gap-1">
          {INTERVAL_OPTIONS.map((h) => (
            <Badge
              key={h}
              bg={data?.interval_hours === h ? 'primary' : 'secondary'}
              style={{ cursor: 'pointer' }}
              onClick={() => handleInterval(h)}
            >
              {h}h
            </Badge>
          ))}
        </div>
      </Form.Group>

      {error && <Alert variant="danger" className="py-1 small mt-2">{error}</Alert>}
    </div>
  );
}
