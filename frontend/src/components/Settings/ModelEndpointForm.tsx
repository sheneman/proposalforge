import { useState, useEffect } from 'react';
import { Form, Button, Alert, Spinner, InputGroup } from 'react-bootstrap';
import { getSettings, saveSettings, testSettings } from '../../api/admin';

interface Props {
  section: string;
  title: string;
  icon: string;
  extraFields?: { key: string; label: string; type?: string; options?: string[] }[];
}

export default function ModelEndpointForm({ section, title, icon, extraFields }: Props) {
  const [values, setValues] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [message, setMessage] = useState<{ type: string; text: string } | null>(null);
  const [showKey, setShowKey] = useState(false);

  useEffect(() => {
    getSettings(section).then((d) => { setValues(d); setLoading(false); });
  }, [section]);

  const handleSave = async () => {
    setSaving(true);
    setMessage(null);
    try {
      await saveSettings(section, values);
      setMessage({ type: 'success', text: 'Settings saved' });
    } catch (e) {
      setMessage({ type: 'danger', text: e instanceof Error ? e.message : 'Save failed' });
    }
    setSaving(false);
  };

  const handleTest = async () => {
    setTesting(true);
    setMessage(null);
    try {
      const res = await testSettings(section, values);
      setMessage({
        type: res.ok ? 'success' : 'danger',
        text: res.ok ? (res.message || 'Connection successful') : (res.error || 'Connection failed'),
      });
    } catch (e) {
      setMessage({ type: 'danger', text: e instanceof Error ? e.message : 'Test failed' });
    }
    setTesting(false);
  };

  if (loading) return <div className="text-center py-3"><Spinner size="sm" /></div>;

  return (
    <div className="border rounded-3 p-3 bg-white mb-3">
      <h6 className="fw-bold mb-3">
        <i className={`bi ${icon} me-2 text-navy`}></i>{title}
      </h6>

      <Form.Group className="mb-2">
        <Form.Label className="small fw-semibold">Base URL</Form.Label>
        <Form.Control size="sm" value={values.base_url || ''} placeholder="https://..."
          onChange={(e) => setValues({ ...values, base_url: e.target.value })} />
      </Form.Group>

      <Form.Group className="mb-2">
        <Form.Label className="small fw-semibold">Model</Form.Label>
        <Form.Control size="sm" value={values.model || ''}
          onChange={(e) => setValues({ ...values, model: e.target.value })} />
      </Form.Group>

      <Form.Group className="mb-2">
        <Form.Label className="small fw-semibold">API Key</Form.Label>
        <InputGroup size="sm">
          <Form.Control type={showKey ? 'text' : 'password'} value={values.api_key || ''}
            onChange={(e) => setValues({ ...values, api_key: e.target.value })} />
          <Button variant="outline-secondary" onClick={() => setShowKey(!showKey)}>
            <i className={`bi ${showKey ? 'bi-eye-slash' : 'bi-eye'}`}></i>
          </Button>
        </InputGroup>
      </Form.Group>

      {extraFields?.map((f) => (
        <Form.Group key={f.key} className="mb-2">
          <Form.Label className="small fw-semibold">{f.label}</Form.Label>
          {f.options ? (
            <Form.Select size="sm" value={values[f.key] || ''}
              onChange={(e) => setValues({ ...values, [f.key]: e.target.value })}>
              {f.options.map((o) => <option key={o} value={o}>{o}</option>)}
            </Form.Select>
          ) : (
            <Form.Control size="sm" type={f.type || 'text'} value={values[f.key] || ''}
              onChange={(e) => setValues({ ...values, [f.key]: e.target.value })} />
          )}
        </Form.Group>
      ))}

      {message && <Alert variant={message.type} className="py-1 small mt-2">{message.text}</Alert>}

      <div className="d-flex gap-2 mt-3">
        <Button size="sm" className="btn-navy" onClick={handleSave} disabled={saving}>
          {saving ? <Spinner size="sm" /> : <><i className="bi bi-check-lg me-1"></i>Save</>}
        </Button>
        <Button size="sm" variant="outline-primary" onClick={handleTest} disabled={testing}>
          {testing ? <Spinner size="sm" /> : <><i className="bi bi-plug me-1"></i>Test</>}
        </Button>
      </div>
    </div>
  );
}
