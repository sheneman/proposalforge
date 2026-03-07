import { useState } from 'react';
import { Form, Button, Alert, Spinner, Row, Col } from 'react-bootstrap';
import { useApi } from '../../hooks/useApi';
import { getCollabNetScheduler, toggleCollabNetScheduler, setCollabNetSchedule } from '../../api/admin';

const DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];

export default function CollabNetScheduler() {
  const { data, loading, refresh } = useApi(getCollabNetScheduler);
  const [day, setDay] = useState('Monday');
  const [hour, setHour] = useState(2);
  const [minute, setMinute] = useState(0);
  const [error, setError] = useState<string | null>(null);

  // Sync local state when data loads
  if (data && day === 'Monday' && data.schedule_day) {
    setDay(data.schedule_day);
    setHour(data.schedule_hour ?? 2);
    setMinute(data.schedule_minute ?? 0);
  }

  const handleToggle = async () => {
    setError(null);
    try {
      await toggleCollabNetScheduler();
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed');
    }
  };

  const handleSave = async () => {
    setError(null);
    try {
      await setCollabNetSchedule({ day, hour, minute });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed');
    }
  };

  if (loading) return <div className="text-center py-3"><Spinner size="sm" /></div>;

  return (
    <div className="border rounded-3 p-3 bg-white mb-3">
      <h6 className="fw-bold mb-3">
        <i className="bi bi-calendar-event me-2 text-navy"></i>CollabNet Scheduler
      </h6>

      <div className="d-flex align-items-center gap-2 mb-3">
        <Button size="sm" variant={data?.enabled ? 'success' : 'outline-secondary'} onClick={handleToggle}>
          {data?.enabled ? 'Enabled' : 'Disabled'}
        </Button>
        {data?.next_run && (
          <small className="text-muted">Next: {new Date(data.next_run).toLocaleString()}</small>
        )}
      </div>

      <Row className="g-2">
        <Col xs={4}>
          <Form.Select size="sm" value={day} onChange={(e) => setDay(e.target.value)}>
            {DAYS.map((d) => <option key={d} value={d.toLowerCase()}>{d}</option>)}
          </Form.Select>
        </Col>
        <Col xs={3}>
          <Form.Control size="sm" type="number" min={0} max={23} value={hour}
            onChange={(e) => setHour(Number(e.target.value))} placeholder="Hour" />
        </Col>
        <Col xs={3}>
          <Form.Control size="sm" type="number" min={0} max={59} value={minute}
            onChange={(e) => setMinute(Number(e.target.value))} placeholder="Min" />
        </Col>
        <Col xs={2}>
          <Button size="sm" className="btn-navy w-100" onClick={handleSave}>Save</Button>
        </Col>
      </Row>

      {error && <Alert variant="danger" className="py-1 small mt-2">{error}</Alert>}
    </div>
  );
}
