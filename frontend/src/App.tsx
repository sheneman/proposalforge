import { Row, Col } from 'react-bootstrap';
import AdminLayout from './components/Layout/AdminLayout';
import GrantsPanel from './components/DataSources/GrantsPanel';
import CollabNetPanel from './components/DataSources/CollabNetPanel';
import SyncHistoryTable from './components/SyncHistory/SyncHistoryTable';
import GrantsScheduler from './components/Scheduler/GrantsScheduler';
import CollabNetScheduler from './components/Scheduler/CollabNetScheduler';
import ModelEndpointForm from './components/Settings/ModelEndpointForm';
import OCRSettings from './components/Settings/OCRSettings';
import AppSettings from './components/Settings/AppSettings';

function SectionHeader({ number, title, icon }: { number: string; title: string; icon: string }) {
  return (
    <h5 className="fw-bold mb-3 mt-2 text-navy">
      <i className={`bi ${icon} me-2`}></i>
      <span className="text-muted me-2" style={{ fontSize: '0.85em' }}>{number}.</span>
      {title}
    </h5>
  );
}

export default function App() {
  return (
    <AdminLayout>
      {/* Section 1: Data Sources */}
      <SectionHeader number="1" title="Data Sources" icon="bi-database" />
      <Row className="g-4 mb-4">
        <Col lg={7}>
          <GrantsPanel />
        </Col>
        <Col lg={5}>
          <CollabNetPanel />
        </Col>
      </Row>

      <hr className="my-4" />

      {/* Section 2: Sync History */}
      <SectionHeader number="2" title="Sync History" icon="bi-clock-history" />
      <div className="mb-4">
        <SyncHistoryTable />
      </div>

      <hr className="my-4" />

      {/* Section 3: Scheduling */}
      <SectionHeader number="3" title="Scheduling" icon="bi-calendar-event" />
      <Row className="g-4 mb-4">
        <Col md={6}><GrantsScheduler /></Col>
        <Col md={6}><CollabNetScheduler /></Col>
      </Row>

      <hr className="my-4" />

      {/* Section 4: Model Endpoints */}
      <SectionHeader number="4" title="Model Endpoints" icon="bi-cpu" />
      <Row className="g-4 mb-4">
        <Col md={6}>
          <ModelEndpointForm section="llm" title="LLM Endpoint" icon="bi-chat-dots" />
        </Col>
        <Col md={6}>
          <ModelEndpointForm section="embedding" title="Embedding Endpoint" icon="bi-vector-pen" />
        </Col>
        <Col md={6}>
          <ModelEndpointForm section="reranker" title="Re-Ranker Endpoint" icon="bi-sort-numeric-down" />
        </Col>
        <Col md={6}>
          <OCRSettings />
        </Col>
      </Row>

      <hr className="my-4" />

      {/* Section 5: App Settings */}
      <SectionHeader number="5" title="App Settings" icon="bi-gear" />
      <Row className="mb-4">
        <Col md={6}><AppSettings /></Col>
      </Row>
    </AdminLayout>
  );
}
