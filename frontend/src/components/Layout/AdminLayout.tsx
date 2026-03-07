import { Container, Nav, Navbar } from 'react-bootstrap';

interface Props {
  children: React.ReactNode;
}

export default function AdminLayout({ children }: Props) {
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
              <Nav.Link href="/">Home</Nav.Link>
              <Nav.Link href="/search">Search</Nav.Link>
              <Nav.Link href="/analytics">Analytics</Nav.Link>
              <Nav.Link href="/researchers">Researchers</Nav.Link>
              <Nav.Link active className="text-gold">Admin</Nav.Link>
            </Nav>
            <Nav>
              <Nav.Link href="/admin/logout">
                <i className="bi bi-box-arrow-right me-1"></i>Logout
              </Nav.Link>
            </Nav>
          </Navbar.Collapse>
        </Container>
      </Navbar>
      <Container fluid className="py-4 px-4">
        {children}
      </Container>
    </>
  );
}
