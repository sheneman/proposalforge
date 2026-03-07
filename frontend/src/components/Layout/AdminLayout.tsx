import { Container, Nav, Navbar } from 'react-bootstrap';

interface Props {
  activeTab: string;
  onTabChange: (tab: string) => void;
  children: React.ReactNode;
}

const TABS = [
  { key: 'datasources', label: 'Data Sources', icon: 'bi-database' },
  { key: 'history', label: 'History', icon: 'bi-clock-history' },
  { key: 'scheduling', label: 'Scheduling', icon: 'bi-calendar-event' },
  { key: 'models', label: 'Models', icon: 'bi-cpu' },
  { key: 'settings', label: 'Settings', icon: 'bi-gear' },
];

export default function AdminLayout({ activeTab, onTabChange, children }: Props) {
  return (
    <>
      <Navbar className="bg-navy" variant="dark" expand="lg">
        <Container fluid>
          <Navbar.Brand href="/" className="text-gold fw-bold">
            <i className="bi bi-building me-2"></i>ProposalForge
          </Navbar.Brand>
          <Navbar.Toggle />
          <Navbar.Collapse>
            <Nav className="me-auto">
              {TABS.map((t) => (
                <Nav.Link
                  key={t.key}
                  active={activeTab === t.key}
                  onClick={() => onTabChange(t.key)}
                  className={activeTab === t.key ? 'text-gold' : ''}
                >
                  <i className={`${t.icon} me-1`}></i>{t.label}
                </Nav.Link>
              ))}
            </Nav>
            <Nav>
              <Nav.Link href="/admin/legacy" className="text-muted small">
                Legacy Admin
              </Nav.Link>
              <Nav.Link href="/admin/logout">
                <i className="bi bi-box-arrow-right me-1"></i>Logout
              </Nav.Link>
            </Nav>
          </Navbar.Collapse>
        </Container>
      </Navbar>
      <Container fluid className="py-4">
        {children}
      </Container>
    </>
  );
}
