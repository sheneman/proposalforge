import { useState } from 'react';
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

export default function App() {
  const [activeTab, setActiveTab] = useState('datasources');

  return (
    <AdminLayout activeTab={activeTab} onTabChange={setActiveTab}>
      {activeTab === 'datasources' && (
        <Row className="g-4">
          <Col lg={7}>
            <GrantsPanel />
          </Col>
          <Col lg={5}>
            <CollabNetPanel />
          </Col>
        </Row>
      )}

      {activeTab === 'history' && <SyncHistoryTable />}

      {activeTab === 'scheduling' && (
        <Row className="g-4">
          <Col md={6}><GrantsScheduler /></Col>
          <Col md={6}><CollabNetScheduler /></Col>
        </Row>
      )}

      {activeTab === 'models' && (
        <Row className="g-4">
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
      )}

      {activeTab === 'settings' && (
        <Row>
          <Col md={6}><AppSettings /></Col>
        </Row>
      )}
    </AdminLayout>
  );
}
