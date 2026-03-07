import { Form } from 'react-bootstrap';

const OPP_TYPES = [
  { value: 'posted', label: 'Posted' },
  { value: 'forecasted', label: 'Forecasted' },
  { value: 'closed', label: 'Closed' },
  { value: 'archived', label: 'Archived' },
];

interface Props {
  selected: string[];
  onChange: (types: string[]) => void;
  disabled?: boolean;
}

export default function TypeSelector({ selected, onChange, disabled }: Props) {
  const toggle = (value: string) => {
    if (selected.includes(value)) {
      onChange(selected.filter((t) => t !== value));
    } else {
      onChange([...selected, value]);
    }
  };

  return (
    <div className="d-flex align-items-center gap-3 flex-wrap">
      <span className="text-muted small fw-semibold">Opportunity types:</span>
      {OPP_TYPES.map((t) => (
        <Form.Check
          key={t.value}
          inline
          type="checkbox"
          id={`type-${t.value}`}
          label={t.label}
          checked={selected.includes(t.value)}
          onChange={() => toggle(t.value)}
          disabled={disabled}
        />
      ))}
    </div>
  );
}
