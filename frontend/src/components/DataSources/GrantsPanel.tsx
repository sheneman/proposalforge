import { Row, Col, Card, Spinner } from 'react-bootstrap';
import PipelineWorkflow from '../Pipeline/PipelineWorkflow';
import { useApi } from '../../hooks/useApi';
import { getGrantsHealth } from '../../api/admin';

export default function GrantsPanel() {
  const { data: health, loading } = useApi(getGrantsHealth);

  return (
    <div>
      <PipelineWorkflow />

      <h6 className="mt-4 mb-3 fw-bold">
        <i className="bi bi-heart-pulse me-2 text-navy"></i>Data Health
      </h6>
      {loading ? (
        <div className="text-center py-3"><Spinner size="sm" /></div>
      ) : health ? (
        <Row className="g-2">
          {Object.entries(health).map(([key, val]) => (
            <Col xs={6} md={3} key={key}>
              <Card className="text-center p-2 h-100">
                <div className="fw-bold" style={{ fontSize: '1.1rem' }}>
                  {typeof val === 'number' ? val.toLocaleString() : val}
                </div>
                <small className="text-muted text-capitalize">
                  {key.replace(/_/g, ' ')}
                </small>
              </Card>
            </Col>
          ))}
        </Row>
      ) : null}
    </div>
  );
}
