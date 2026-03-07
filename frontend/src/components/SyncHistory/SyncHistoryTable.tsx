import { Table, Badge, Spinner } from 'react-bootstrap';
import { useApi } from '../../hooks/useApi';
import { getSyncHistory } from '../../api/admin';
import type { SyncLogEntry } from '../../types';

const STATUS_BADGES: Record<string, string> = {
  completed: 'success',
  failed: 'danger',
  running: 'warning',
  cancelled: 'secondary',
};

export default function SyncHistoryTable() {
  const { data, loading } = useApi(getSyncHistory);

  if (loading) return <div className="text-center py-4"><Spinner size="sm" /></div>;

  const logs: SyncLogEntry[] = data || [];

  return (
    <div className="border rounded-3 p-3 bg-white">
      <h6 className="fw-bold mb-3">
        <i className="bi bi-clock-history me-2 text-navy"></i>Sync History
      </h6>
      {logs.length === 0 ? (
        <p className="text-muted small">No sync history yet.</p>
      ) : (
        <div className="table-responsive">
          <Table size="sm" hover className="mb-0">
            <thead>
              <tr>
                <th>Type</th>
                <th>Status</th>
                <th>Started</th>
                <th>Duration</th>
                <th>Items</th>
                <th>Errors</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {logs.map((log) => (
                <tr key={log.id}>
                  <td><Badge bg="info" className="text-capitalize">{log.sync_type}</Badge></td>
                  <td><Badge bg={STATUS_BADGES[log.status] || 'secondary'}>{log.status}</Badge></td>
                  <td className="small">{new Date(log.started_at).toLocaleString()}</td>
                  <td className="small">
                    {log.duration_seconds != null ? `${Math.round(log.duration_seconds)}s` : '—'}
                  </td>
                  <td>{log.success_count}/{log.total_items}</td>
                  <td className={log.error_count > 0 ? 'text-danger' : ''}>{log.error_count}</td>
                  <td className="small text-truncate" style={{ maxWidth: 200 }}>{log.error_message || ''}</td>
                </tr>
              ))}
            </tbody>
          </Table>
        </div>
      )}
    </div>
  );
}
