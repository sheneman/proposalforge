import { useState, useEffect } from 'react';
import { Form, Button, Alert, Spinner } from 'react-bootstrap';
import { getSettings, saveSettings } from '../../api/admin';

const TIMEZONE_CHOICES = [
  'US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific',
  'US/Alaska', 'US/Hawaii', 'UTC',
];

export default function AppSettings() {
  const [timezone, setTimezone] = useState('UTC');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<{ type: string; text: string } | null>(null);

  useEffect(() => {
    getSettings('app').then((d) => { setTimezone(d.timezone || 'UTC'); setLoading(false); });
  }, []);

  const handleSave = async () => {
    setSaving(true);
    setMessage(null);
    try {
      await saveSettings('app', { timezone });
      setMessage({ type: 'success', text: 'Timezone saved' });
    } catch (e) {
      setMessage({ type: 'danger', text: e instanceof Error ? e.message : 'Failed' });
    }
    setSaving(false);
  };

  if (loading) return <div className="text-center py-3"><Spinner size="sm" /></div>;

  return (
    <div className="border rounded-3 p-3 bg-white">
      <h6 className="fw-bold mb-3">
        <i className="bi bi-globe me-2 text-navy"></i>Application Settings
      </h6>
      <Form.Group className="mb-3">
        <Form.Label className="small fw-semibold">Display Timezone</Form.Label>
        <Form.Select size="sm" value={timezone} onChange={(e) => setTimezone(e.target.value)}>
          {TIMEZONE_CHOICES.map((tz) => <option key={tz} value={tz}>{tz}</option>)}
        </Form.Select>
      </Form.Group>
      {message && <Alert variant={message.type} className="py-1 small">{message.text}</Alert>}
      <Button size="sm" className="btn-navy" onClick={handleSave} disabled={saving}>
        {saving ? <Spinner size="sm" /> : <><i className="bi bi-check-lg me-1"></i>Save</>}
      </Button>
    </div>
  );
}
